"""Edge cases of leases, terms and removals found by review.

Tests against a real NATS JetStream server.
"""

import asyncio
import logging
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

from nats.js.errors import KeyNotFoundError, KeyWrongLastSequenceError
import pytest

import homeassistant.cluster as cluster_module
from homeassistant.cluster.const import LEADER_KEY
from homeassistant.cluster.coordinator import ClusterCoordinator, NoTerm
from homeassistant.config_entries import ConfigEntryState
from homeassistant.helpers.json import json_bytes

from .conftest import NodeFactory
from .test_init import wait_for

LOADED = ConfigEntryState.LOADED


async def test_transient_read_error_while_reclaiming_does_not_demote(
    make_node: NodeFactory,
) -> None:
    """One failed read after a lost renewal reply is not a lost lease."""
    rec = make_node.recorder
    a = await make_node("node_a")
    leases = a.cluster.leases
    kv = a.cluster.coordinator.kv_leases
    real_update, real_get = kv.update, kv.get
    calls = {"update": 0, "get": 0}

    async def update(key: str, value: bytes, last: int | None = None, **kw: object):
        if key == LEADER_KEY and calls["update"] == 0:
            calls["update"] += 1
            raise KeyWrongLastSequenceError
        return await real_update(key, value, last=last, **kw)

    async def get(key: str, *args: object, **kw: object):
        if key == LEADER_KEY and calls["get"] == 0 and calls["update"] == 1:
            calls["get"] += 1
            raise TimeoutError
        return await real_get(key, *args, **kw)

    with patch.object(kv, "update", update), patch.object(kv, "get", get):
        await wait_for(lambda: calls["get"] == 1, timeout=5)
        await asyncio.sleep(1.5)

    assert leases.is_leader
    assert a.entry.state is LOADED
    assert not rec.times("node_a", "unload")
    assert a.cluster.serves_as_leader  # the reclaim kept the endpoint at 200


async def test_lease_not_confirmed_after_bucket_loss_is_not_deleted_unconditionally(
    make_node: NodeFactory,
) -> None:
    """A revision of -1 must never become a delete without a condition."""
    a = await make_node("node_a")
    leases = a.cluster.leases
    kv = a.cluster.coordinator.kv_leases
    leases._held[LEADER_KEY].revision = -1
    await kv.put(LEADER_KEY, json_bytes({"node": "node_x", "incarnation": "x"}))

    with patch.object(leases, "_is_quiescent", return_value=True):
        await leases._async_release(LEADER_KEY)

    assert b"node_x" in (await kv.get(LEADER_KEY)).value


async def test_tombstone_is_not_reported_done_by_a_node_that_does_not_lead(
    make_node: NodeFactory,
) -> None:
    """Only a leader can confirm a removal; the announcement and intent stay."""
    await make_node("node_a")
    b = await make_node("node_b")
    storage = b.cluster.storage
    assert not b.cluster.leases.is_leader

    assert not await storage._async_publish_tombstone("gone", "cluster_dev")

    storage._removing.add("gone")
    storage._pending_tombstones["gone"] = "cluster_dev"
    await storage._async_drain()

    assert "gone" in storage._removing
    assert storage._deferred_tombstones == {"gone": "cluster_dev"}


async def test_term_is_started_again_after_transient_errors(
    make_node: NodeFactory,
) -> None:
    """A timeout while starting a term again is retried, not dropped."""
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator
    real = coordinator.async_start_term
    failures: list[int] = []

    async def flaky(node_id: str) -> int:
        if len(failures) < 2:
            failures.append(1)
            raise TimeoutError
        return await real(node_id)

    coordinator.fence_revision = None
    with patch.object(coordinator, "async_start_term", flaky):
        await a.cluster._async_restart_term()

    assert len(failures) == 2
    assert coordinator.fence_revision is not None
    assert a.cluster.leases.is_leader


async def test_leader_that_cannot_get_a_term_stops_leading(
    make_node: NodeFactory,
) -> None:
    """A leader whose writes can never be accepted gives way to another node."""
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator

    async def failing(node_id: str) -> int:
        raise TimeoutError

    with (
        patch.object(coordinator, "async_start_term", failing),
        patch.object(cluster_module, "_TERM_RESTART_ATTEMPTS", 2),
    ):
        await a.cluster._async_restart_term()

    await wait_for(lambda: not a.cluster.leases.is_leader, timeout=5)


async def test_write_without_a_term_asks_for_a_check(make_node: NodeFactory) -> None:
    """No term means nothing is sent, so nothing else would notice the loss."""
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator
    coordinator.fence_revision = None
    coordinator._last_check = 0.0
    coordinator.on_bucket_check = check = MagicMock()

    with pytest.raises(NoTerm):
        await coordinator.async_fenced_put("entry.x", b"1")

    check.assert_called_once()


async def test_joining_an_existing_bucket_is_not_a_warning(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """Every node but the first binds to the buckets: that is normal."""
    caplog.set_level(logging.WARNING, logger="homeassistant.cluster")
    await make_node("node_a")
    caplog.clear()

    await make_node("node_b")

    assert "whose configuration differs" not in caplog.text


async def test_joining_a_bucket_with_other_settings_warns(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """A node with another lease_ttl than the first one is told so."""
    caplog.set_level(logging.WARNING, logger="homeassistant.cluster")
    await make_node("node_a", lease_ttl=3.0, renew_interval=1.0, unload_grace=2.0)
    caplog.clear()

    await make_node("node_b", lease_ttl=4.0, renew_interval=1.0, unload_grace=2.0)

    assert "whose configuration differs" in caplog.text


async def test_oversized_entry_record_does_not_block_the_flush(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """A record the server can never accept is reported once, not retried forever."""
    caplog.set_level(logging.ERROR, logger="homeassistant.cluster")
    a = await make_node("node_a")
    storage = a.cluster.storage
    storage._last_hash.clear()

    with patch.object(
        ClusterCoordinator, "max_payload", new_callable=PropertyMock, return_value=300
    ):
        assert await storage._async_publish_entry(a.entry) is True
        storage._last_hash.clear()
        assert await storage._async_publish_entry(a.entry) is True
        await storage.async_flush()

    assert caplog.text.count("larger than the NATS max_payload") == 1


async def test_unchanged_registry_is_in_the_cluster_when_the_leader_flushes(
    make_node: NodeFactory, tmp_path: Path
) -> None:
    """Core writes a registry only when it changes: the saved file is published."""
    a = await make_node("node_a")
    storage_dir = tmp_path / ".storage"
    storage_dir.mkdir()
    saved = '{"version": 1, "minor_version": 1, "key": "core.area_registry", "data": {"areas": []}}'
    (storage_dir / "core.area_registry").write_text(saved)
    a.hass.config.config_dir = str(tmp_path)
    kv = a.cluster.coordinator.kv_storage

    await a.cluster.storage.async_flush()

    assert (await kv.get("store.core.area_registry")).value


async def test_saved_file_does_not_replace_the_previous_leaders_record(
    make_node: NodeFactory, tmp_path: Path
) -> None:
    """After a takeover the saved file is the node's old copy: the record stays."""
    a = await make_node("node_a")
    storage_dir = tmp_path / ".storage"
    storage_dir.mkdir()
    (storage_dir / "core.label_registry").write_text(
        '{"version": 1, "minor_version": 1, "key": "core.label_registry",'
        ' "data": {"labels": ["old copy"]}}'
    )
    a.hass.config.config_dir = str(tmp_path)
    kv = a.cluster.coordinator.kv_storage
    record = b'{"origin": "node_x", "content": {"data": {"labels": ["newer"]}}}'
    await kv.put("store.core.label_registry", record)

    await a.cluster.storage.async_flush()

    assert (await kv.get("store.core.label_registry")).value == record


@pytest.mark.parametrize("other_writer", [False, True])
async def test_release_after_a_renewal_landed_late(
    make_node: NodeFactory, other_writer: bool
) -> None:
    """A renewal stored after the loop was cancelled does not keep the keys.

    ``async_stop`` cancels the lease loop; a renewal that was already on the
    wire may still be stored, so the revision the node knows is one behind.
    The clean release still deletes its own keys (else the next start of the
    node waits for them to expire, or takes itself for a duplicate), but
    never a key another incarnation wrote.
    """
    a = await make_node("node_a")
    await wait_for(lambda: a.entry.state is LOADED, timeout=15)
    assert await a.hass.config_entries.async_unload(a.entry.entry_id)
    kv = a.cluster.coordinator.kv_leases
    real_delete = kv.delete
    bumped: list[str] = []

    async def delete(key: str, last: int | None = None, **kw: object):
        if key not in bumped:
            bumped.append(key)
            # The late renewal, stored just before the delete arrives.
            current = await kv.get(key)
            value = (
                json_bytes({"node": "node_a", "incarnation": "a later run"})
                if other_writer
                else current.value
            )
            await kv.update(key, value, last=current.revision)
        return await real_delete(key, last=last, **kw)

    member_key = a.cluster.leases.member_key
    with patch.object(kv, "delete", delete):
        await a.cluster.async_release()
    assert bumped == [LEADER_KEY, member_key]
    for key in bumped:
        if other_writer:
            assert (await kv.get(key)).value  # left for its writer
        else:
            with pytest.raises(KeyNotFoundError):
                await kv.get(key)
