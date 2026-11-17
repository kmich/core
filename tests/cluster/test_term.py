"""The leadership term fences writes to hass_storage, against a real NATS server."""

import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import nats
from nats.aio.client import ServerVersion
from nats.js.errors import APIError, KeyNotFoundError
import pytest

from homeassistant.cluster.const import BUCKET_STORAGE, TERM_KEY
from homeassistant.cluster.coordinator import ClusterCoordinator, NoTerm, TermSuperseded
from homeassistant.config_entries import ConfigEntryState
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import cluster as cluster_helper
from homeassistant.helpers.json import json_bytes
from homeassistant.util.json import json_loads_object

from .conftest import DEV_ENTRY_ID, NodeFactory
from .test_init import wait_for

LOADED = ConfigEntryState.LOADED


@pytest.mark.parametrize(
    ("version", "accepted"),
    [("2.15.3", True), ("2.16.0", True), ("2.14.9", False), ("garbage", False)],
)
def test_server_version_check(version: str, accepted: bool) -> None:
    """A NATS server older than the tested line is refused."""
    coordinator = ClusterCoordinator("nats://127.0.0.1:4222", "node_a", lease_ttl=3.0)
    coordinator.nc = Mock(connected_server_version=ServerVersion(version))
    if accepted:
        coordinator._check_server_version()
    else:
        with pytest.raises(HomeAssistantError, match=r"needs NATS 2\.15 or later"):
            coordinator._check_server_version()


async def _term(node_coordinator: ClusterCoordinator) -> tuple[dict, int]:
    kv = node_coordinator.kv_storage
    assert kv is not None
    current = await kv.get(TERM_KEY)
    return json_loads_object(current.value), current.revision


async def test_each_leader_starts_a_term(make_node: NodeFactory) -> None:
    """A promotion increases the term; the next leader's is one higher."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    term, revision = await _term(a.cluster.coordinator)
    assert term["leader"] == "node_a"
    assert a.cluster.coordinator.fence_revision == revision
    assert b.cluster.coordinator.fence_revision is None
    record = json_loads_object(
        (await a.cluster.coordinator.kv_storage.get(f"entry.{DEV_ENTRY_ID}")).value
    )
    assert record["term"] == term["term"]

    await a.hass.async_stop()
    await wait_for(lambda: b.entry.state is LOADED, timeout=15)
    later, _ = await _term(b.cluster.coordinator)
    assert later == {"term": term["term"] + 1, "leader": "node_b"}


async def test_write_of_a_superseded_term_is_refused(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """Once another node started a term, the server refuses the old leader.

    The old leader stops leading at once (the refusal is the evidence that
    another node took over) and writes nothing.
    """
    caplog.set_level(logging.INFO, logger="homeassistant.cluster")
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator
    kv = coordinator.kv_storage
    assert kv is not None
    stale = coordinator.fence_revision
    term, revision = await _term(coordinator)
    leadership: list[bool] = []
    cluster_helper.async_track_leadership(a.hass, leadership.append)

    # What node_b's promotion does.
    await kv.update(
        TERM_KEY,
        json_bytes({"term": term["term"] + 1, "leader": "node_b"}),
        last=revision,
    )

    with pytest.raises(TermSuperseded):
        await coordinator.async_fenced_put("entry.stale_write", b"{}")
    with pytest.raises(KeyNotFoundError):
        await kv.get("entry.stale_write")
    assert coordinator.fence_revision is None or coordinator.fence_revision != stale
    await wait_for(lambda: False in leadership, timeout=5)
    assert "was superseded by term" in caplog.text
    assert make_node.recorder.times("node_a", "unload")


async def test_lost_term_key_retries_instead_of_demoting(
    make_node: NodeFactory,
) -> None:
    """A term key that is gone (not moved) is no other node's term."""
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator
    nc = await nats.connect(make_node.url)
    await (await nc.jetstream().key_value(BUCKET_STORAGE)).purge(TERM_KEY)
    await nc.close()

    with pytest.raises(NoTerm):
        await coordinator.async_fenced_put("entry.after_loss", b"{}")
    await asyncio.sleep(1)
    assert a.entry.state is LOADED
    assert cluster_helper.async_is_leader(a.hass)
    assert not make_node.recorder.times("node_a", "unload")
    # The bucket check finds the term key gone and starts a term again.
    await wait_for(lambda: coordinator.fence_revision is not None, timeout=10)
    await coordinator.async_fenced_put("entry.after_loss", b"{}")


async def test_storage_bucket_recreated_starts_a_term_again(
    make_node: NodeFactory,
) -> None:
    """The leader of a recreated storage bucket writes again, in a new term."""
    a = await make_node("node_a")
    nc = await nats.connect(make_node.url)
    await nc.jetstream().delete_stream(f"KV_{BUCKET_STORAGE}")
    await nc.close()

    async def republished() -> bool:
        kv = a.cluster.coordinator.kv_storage
        try:
            await kv.get(TERM_KEY)
            await kv.get(f"entry.{DEV_ENTRY_ID}")
        except Exception:  # noqa: BLE001 - not there yet
            return False
        return True

    for _ in range(100):
        if await republished():
            break
        await asyncio.sleep(0.2)
    else:
        pytest.fail("the leader never wrote to the recreated bucket")
    term, revision = await _term(a.cluster.coordinator)
    assert term["leader"] == "node_a"
    assert a.cluster.coordinator.fence_revision == revision
    assert a.entry.state is LOADED
    assert not make_node.recorder.times("node_a", "unload")


@pytest.mark.parametrize(
    ("reported", "later_read", "expected"),
    [
        pytest.param(9, 9, TermSuperseded, id="newer_term_still_there"),
        pytest.param(9, 12, NoTerm, id="newer_term_replaced_before_the_read"),
        pytest.param(9, None, NoTerm, id="newer_term_gone_before_the_read"),
        pytest.param(5, 5, NoTerm, id="refused_for_another_reason"),
        pytest.param(0, 9, NoTerm, id="key_missing_then_replaced_before_the_read"),
    ],
)
async def test_refusal_is_classified_by_what_the_server_saw(
    reported: int, later_read: int | None, expected: type[Exception]
) -> None:
    """A read that shows another state than the refusal named must not decide."""
    coordinator = ClusterCoordinator("nats://127.0.0.1:4222", "node_a", lease_ttl=3.0)
    coordinator.fence_revision = 5
    coordinator.kv_storage = Mock()
    if later_read is None:
        coordinator.kv_storage.get = AsyncMock(
            side_effect=KeyNotFoundError(entry=Mock(revision=20))
        )
    else:
        coordinator.kv_storage.get = AsyncMock(
            return_value=Mock(revision=later_read, value=b"{}")
        )
    refusal = APIError(
        code=400, err_code=10071, description=f"wrong last sequence: {reported}"
    )

    with pytest.raises(expected):
        await coordinator._async_refused(5, refusal)
    assert coordinator.fence_revision == (None if expected is TermSuperseded else 5)


async def test_recreated_bucket_does_not_reuse_revisions(
    make_node: NodeFactory,
) -> None:
    """A stream replaced under the same name starts above the old sequences.

    A compare-and-set condition naming a sequence of the old stream must not
    be satisfiable by the new one.
    """
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator
    first = await coordinator.kv_storage.put("probe", b"1")
    nc = await nats.connect(make_node.url)
    await nc.jetstream().delete_stream(f"KV_{BUCKET_STORAGE}")
    await nc.close()
    await asyncio.sleep(0.01)

    kv = await coordinator._bucket(coordinator._config(BUCKET_STORAGE))
    second = await kv.put("probe", b"1")

    assert second > first
    assert second > 1_000_000_000
