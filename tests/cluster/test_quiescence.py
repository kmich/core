"""A node releases the lead lease only once all its work stopped.

Tests against a real NATS JetStream server.
"""

import asyncio
import logging
import time
from typing import Any
from unittest.mock import patch

import nats
from nats.js.api import KeyValueConfig, StorageType
import pytest

from homeassistant.cluster.const import LEADER_KEY
from homeassistant.cluster.lease_manager import LeaseManager
from homeassistant.config_entries import (
    SOURCE_REAUTH,
    ConfigEntryDisabler,
    ConfigEntryState,
    OperationNotAllowed,
)
from homeassistant.core import ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.json import json_bytes

from .conftest import DEV_ENTRY_ID, HardExit, NodeFactory, start_node
from .test_failure_modes import install_custom
from .test_init import wait_for

LOADED = ConfigEntryState.LOADED
NOT_LOADED = ConfigEntryState.NOT_LOADED


async def test_hand_off_after_a_failed_unload_does_not_release(
    make_node: NodeFactory, hard_exit: HardExit
) -> None:
    """An unload that failed during a hand-off leaves the lease to expire.

    The other node must not start the entry before this node's watchdog ends
    it: a deleted key would have let it start at once.
    """
    rec = make_node.recorder
    a = await make_node("node_a", start=False)
    install_custom(a.hass, "node_a", rec, unload_raises=True)
    with patch.object(LeaseManager, "async_restart") as restart:
        await start_node(a, make_node.url)
        b = await make_node("node_b")
        assert a.entry.state is LOADED
        kv = a.cluster.coordinator.kv_leases

        await a.cluster.leases._async_hand_off_leader()

        assert restart.called
        assert a.entry.state is ConfigEntryState.FAILED_UNLOAD
        # The key was not deleted: it still names node_a until it expires.
        assert b"node_a" in (await kv.get(LEADER_KEY)).value
        await asyncio.sleep(1)
        assert b.entry.state is NOT_LOADED

        await wait_for(lambda: b.entry.state is LOADED, timeout=20)

    assert hard_exit.times, "the watchdog of node_a never fired"
    assert hard_exit.times[0] < rec.times("node_b", "setup")[-1]


async def test_stalled_hand_off_does_not_keep_the_lease_alive(
    make_node: NodeFactory, hard_exit: HardExit
) -> None:
    """A hand-off that never finishes its unload stops renewing the lease.

    Renewals would push the watchdog back for as long as the unload hangs, and
    no other node could take over.
    """
    # The test's timings assume the default timers.
    a = await make_node("node_a", lease_ttl=3.0, renew_interval=1.0, unload_grace=2.0)
    # The hand-off's cool-down needs another member.
    await make_node("node_b", lease_ttl=3.0, renew_interval=1.0, unload_grace=2.0)
    kv = a.cluster.coordinator.kv_leases
    revision = (await kv.get(LEADER_KEY)).revision
    stuck = asyncio.Event()

    async def hang(entry: Any) -> None:
        await stuck.wait()

    with patch.object(a.cluster.leases, "_async_unload_entry", hang):
        hand_off = asyncio.create_task(a.cluster.leases._async_hand_off_leader())
        await asyncio.sleep(1.2)  # more than a renewal interval, less than the TTL
        assert (await kv.get(LEADER_KEY)).revision == revision
        await wait_for(lambda: bool(hard_exit.times), timeout=10)
        stuck.set()
        await hand_off


async def test_shutdown_after_a_failed_unload_does_not_release(
    make_node: NodeFactory, hard_exit: HardExit, caplog: pytest.LogCaptureFixture
) -> None:
    """A leader that could not unload while stopping leaves the lease to expire."""
    caplog.set_level(logging.INFO, logger="homeassistant.cluster")
    rec = make_node.recorder
    b = await make_node("node_b", start=False)
    install_custom(b.hass, "node_b", rec, unload_raises=True)
    await start_node(b, make_node.url)
    a = await make_node("node_a")
    assert (a.entry.state, b.entry.state) == (NOT_LOADED, LOADED)

    with patch.object(LeaseManager, "async_restart") as restart:
        await b.hass.async_stop()
    assert not restart.called  # stopping already
    assert "stops without releasing" in caplog.text

    await wait_for(lambda: a.entry.state is LOADED, timeout=20)
    assert f"Node node_a won {LEADER_KEY} after" in caplog.text
    assert hard_exit.times, "the watchdog of node_b never fired"
    assert hard_exit.times[0] < rec.times("node_a", "setup")[-1]


async def test_clean_shutdown_unloads_then_releases(
    make_node: NodeFactory, hard_exit: HardExit, caplog: pytest.LogCaptureFixture
) -> None:
    """A stopping leader unloads its entries first; then the takeover is immediate."""
    caplog.set_level(logging.INFO, logger="homeassistant.cluster")
    rec = make_node.recorder
    b = await make_node("node_b")
    a = await make_node("node_a")

    await b.hass.async_stop()
    await wait_for(lambda: a.entry.state is LOADED, timeout=5)

    assert len(rec.times("node_b", "unload")) == 1
    assert rec.times("node_b", "unload")[0] < rec.times("node_a", "setup")[-1]
    assert f"Node node_a won {LEADER_KEY} after" not in caplog.text
    assert not hard_exit.times


async def _renew(kv: Any, payload: bytes, stop: asyncio.Event) -> None:
    while not stop.is_set():
        await kv.put(LEADER_KEY, payload)
        await asyncio.sleep(0.5)


async def test_clean_release_must_be_the_one_that_was_read(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """A release seen before another node's term does not skip the grace.

    The standby reads the delete marker of a clean release; before it writes,
    node_c takes the key again. The standby's write must fail, and once
    node_c's key expired it waits the full grace node_c advertised.
    """
    caplog.set_level(logging.INFO, logger="homeassistant.cluster")
    nc = await nats.connect(make_node.url)
    kv = await nc.jetstream().create_key_value(
        KeyValueConfig(
            bucket="hass_leases",
            history=1,
            ttl=3.0,
            storage=StorageType.MEMORY,
            replicas=1,
        )
    )
    payload = json_bytes({"node": "node_c", "incarnation": "c1", "grace": 5.0})
    stop_renewing = asyncio.Event()
    renewer = asyncio.create_task(_renew(kv, payload, stop_renewing))
    try:
        b = await make_node("node_b")
        await asyncio.sleep(1)
        assert b.entry.state is NOT_LOADED
        leases = b.cluster.leases
        assert leases.takeover_grace == 5.0

        original = leases._async_previous_holder
        raced = False

        async def _previous_holder(key: str) -> tuple[str | None, int | None]:
            nonlocal raced
            result = await original(key)
            if result[0] is None and not raced:
                raced = True
                # node_c takes the key again after the release was read.
                await kv.put(LEADER_KEY, payload)
            return result

        stop_renewing.set()
        await renewer
        with patch.object(leases, "_async_previous_holder", _previous_holder):
            released_at = time.monotonic()
            await kv.delete(LEADER_KEY)
            await wait_for(lambda: raced, timeout=5)
            await asyncio.sleep(0.5)
            assert not leases.is_leader
            assert b.entry.state is NOT_LOADED

            await wait_for(lambda: b.entry.state is LOADED, timeout=20)
        # node_c's last write was right after the release: the key expires
        # 3 s after it, and node_c may still stop for its 5 s grace.
        assert time.monotonic() - released_at >= 3.0 + 5.0 - 0.5
        assert f"Node node_b won {LEADER_KEY} after" in caplog.text
    finally:
        stop_renewing.set()
        await renewer
        await nc.close()


async def test_standby_refuses_to_remove_a_clustered_entry(
    make_node: NodeFactory,
) -> None:
    """Removing an entry runs its clean-up: only the leader does it."""
    rec = make_node.recorder
    a = await make_node("node_a")
    b = await make_node("node_b")
    assert (a.entry.state, b.entry.state) == (LOADED, NOT_LOADED)

    with pytest.raises(OperationNotAllowed, match="runs on cluster node node_a"):
        await b.hass.config_entries.async_remove(DEV_ENTRY_ID)
    assert b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is not None
    assert a.entry.state is LOADED

    await a.hass.config_entries.async_remove(DEV_ENTRY_ID)
    await wait_for(
        lambda: b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is None,
        timeout=10,
    )
    assert not rec.times("node_b", "unload")


async def test_standby_refuses_changes_that_the_takeover_would_revert(
    make_node: NodeFactory,
) -> None:
    """Options, reauth and disabling are made on the leader; a standby refuses."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    assert (a.entry.state, b.entry.state) == (LOADED, NOT_LOADED)
    entries = b.hass.config_entries

    with pytest.raises(OperationNotAllowed, match="runs on cluster node node_a"):
        await entries.options.async_init(DEV_ENTRY_ID)
    with pytest.raises(OperationNotAllowed, match="runs on cluster node node_a"):
        await entries.flow.async_init(
            "dev", context={"source": SOURCE_REAUTH, "entry_id": DEV_ENTRY_ID}
        )
    with pytest.raises(OperationNotAllowed, match="disabled_by"):
        await entries.async_set_disabled_by(DEV_ENTRY_ID, ConfigEntryDisabler.USER)
    assert b.entry.disabled_by is None

    # The leader may disable it, and the standby takes that over: the record of
    # the leader is applied through the same call, and is let through.
    await a.hass.config_entries.async_set_disabled_by(
        DEV_ENTRY_ID, ConfigEntryDisabler.USER
    )
    await wait_for(lambda: b.entry.disabled_by is ConfigEntryDisabler.USER, timeout=10)
    assert not b.cluster.storage.applying_record


async def test_grace_advertised_during_the_wait_extends_it(
    make_node: NodeFactory,
) -> None:
    """A peer's larger unload grace, learned while waiting, moves the barrier."""
    a = await make_node("node_a")
    leases = a.cluster.leases
    now = leases.hass.loop.time()
    leases._grace_until = now + 0.3
    leases._grace_base = (now, now)

    waiter = asyncio.create_task(leases._async_wait_grace(LEADER_KEY))
    await asyncio.sleep(0.1)
    leases._async_note_peer_grace("node_x", 30.0)
    await asyncio.sleep(0.5)

    assert not waiter.done()
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)


async def test_own_previous_run_is_reclaimed_after_the_full_barrier(
    make_node: NodeFactory,
) -> None:
    """The earlier run may still stop its work for a lease and the grace."""
    a = await make_node("node_a")
    leases = a.cluster.leases

    leases._async_note_peer_grace("node_x", 9.0)

    assert leases._reclaim_after == leases.ttl + 9.0


async def test_grace_of_a_holder_that_expired_is_still_waited_for(
    make_node: NodeFactory,
) -> None:
    """A node that never saw the holder's lease still learns its unload grace."""
    a = await make_node("node_a")
    nc = await nats.connect(make_node.url)
    kv = await nc.jetstream().key_value("hass_storage")
    await kv.put("grace.node_x", b"9.0")
    await nc.close()

    b = await make_node("node_b")

    assert b.cluster.leases.takeover_grace == 9.0
    assert a.cluster.leases.takeover_grace == a.cluster.leases.unload_grace


async def test_a_call_made_on_the_leader_is_leader_work(make_node: NodeFactory) -> None:
    """A call made on the leader itself (UI, API) counts like a forwarded one.

    While it runs, the node is not quiescent (no clean release, the watchdog
    stays armed); when leadership ends it gets the grace, then is cancelled,
    and its caller gets an error instead of a call that runs on.
    """
    a = await make_node("node_a")
    await wait_for(lambda: a.entry.state is LOADED, timeout=15)
    started = asyncio.Event()
    finished: list[str] = []

    async def _slow(call: ServiceCall) -> None:
        started.set()
        await asyncio.sleep(30)
        finished.append(call.service)

    a.hass.services.async_register("cluster_test", "slow", _slow)
    call = asyncio.create_task(
        a.hass.services.async_call("cluster_test", "slow", {}, blocking=True)
    )
    await asyncio.wait_for(started.wait(), 5)
    assert a.cluster.router.busy()
    assert not a.cluster.leases._is_quiescent()

    await a.cluster.router.async_drain(0.1)  # what the end of leadership does
    with pytest.raises(HomeAssistantError, match="stopped leading"):
        await call
    assert not finished
    assert not a.cluster.router.busy()

    # A call that is cancelled by its caller is cancelled, not turned into an error.
    started.clear()
    call = asyncio.create_task(
        a.hass.services.async_call("cluster_test", "slow", {}, blocking=True)
    )
    await asyncio.wait_for(started.wait(), 5)
    call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await call
    await wait_for(lambda: not a.cluster.router.busy(), timeout=5)

    # A non-blocking call's service is tracked too.
    started.clear()
    await a.hass.services.async_call("cluster_test", "slow", {}, blocking=False)
    await asyncio.wait_for(started.wait(), 5)
    assert a.cluster.router.busy()
    await a.cluster.router.async_drain(0.1)
    assert not a.cluster.router.busy()
    assert not finished
