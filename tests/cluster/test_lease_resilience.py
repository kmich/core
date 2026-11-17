"""Lost replies, slow listeners, failed handover, watcher leaks."""

import asyncio
import json
from typing import Any

import nats.errors
import pytest

from homeassistant.cluster import addon_gate
from homeassistant.cluster.const import LEADER_KEY
from homeassistant.config_entries import ConfigEntryState

from .conftest import DEV_ENTRY_ID, NodeFactory, open_entry_payload
from .test_init import wait_for


async def test_renewal_applied_but_reply_lost_keeps_lease(
    make_node: NodeFactory,
) -> None:
    """The server applies a renewal but its reply times out.

    The next renewal then fails the revision check against our own write.
    That must not count as losing the lease.
    """
    a = await make_node("node_a")
    kv = a.cluster.coordinator.kv_leases
    original_update = kv.update
    dropped = False

    async def drop_one_reply(
        key: str, value: bytes, last: int | None = None, **kwargs: Any
    ) -> int:
        nonlocal dropped
        revision = await original_update(key, value, last=last, **kwargs)
        if key == LEADER_KEY and not dropped:
            dropped = True
            raise nats.errors.TimeoutError
        return revision

    kv.update = drop_one_reply
    await asyncio.sleep(4)  # longer than the default 3 s TTL

    assert dropped
    assert not make_node.recorder.times("node_a", "unload")
    assert a.entry.state is ConfigEntryState.LOADED
    assert a.cluster.leases.holds(DEV_ENTRY_ID)


async def test_slow_leader_listener_does_not_block_renewals(
    make_node: NodeFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A leader listener slower than the fence (Supervisor starting add-ons)."""
    calls: list[bool] = []

    async def slow_listener(self: Any, leader: bool) -> None:
        calls.append(leader)
        await asyncio.sleep(4)

    monkeypatch.setattr(
        addon_gate.SupervisorAddonGate, "async_on_leader_change", slow_listener
    )
    a = await make_node("node_a")
    await asyncio.sleep(6)

    assert calls == [True]
    assert a.cluster.leases.is_leader


async def test_entry_pull_timeout_does_not_start_from_stale_data(
    make_node: NodeFactory,
) -> None:
    """A timeout while reading the handover record is not "no record".

    The standby must not set the entry up with its stale in-memory data; it
    gives the lease back and retries until the read succeeds.
    """
    a = await make_node("node_a")
    b = await make_node("node_b")
    a.hass.config_entries.async_update_entry(a.entry, data={"token": "t2"})
    await a.hass.async_block_till_done()

    key = f"entry.{DEV_ENTRY_ID}"
    kv_a = a.cluster.coordinator.kv_storage

    async def published() -> bool:
        try:
            raw = (await kv_a.get(key)).value
            return b'"t2"' in json.dumps(open_entry_payload(key, raw)).encode()
        except Exception:  # noqa: BLE001
            return False

    for _ in range(50):
        if await published():
            break
        await asyncio.sleep(0.2)
    else:
        raise AssertionError("entry data was never mirrored")

    kv_b = b.cluster.coordinator.kv_storage
    original_get = kv_b.get
    failures = 0

    async def time_out_twice(k: str, *args: Any, **kwargs: Any) -> Any:
        nonlocal failures
        if k == key and failures < 2:
            failures += 1
            raise nats.errors.TimeoutError
        return await original_get(k, *args, **kwargs)

    kv_b.get = time_out_twice
    await a.cluster.leases.async_stop(release=True)
    await wait_for(lambda: b.entry.state is ConfigEntryState.LOADED, 10)

    assert failures == 2
    b_setups = [
        d for _, n, k, d in make_node.recorder.events if (n, k) == ("node_b", "setup")
    ]
    assert b_setups == [{"token": "t2"}]


async def test_restarting_watchers_does_not_leak_subscriptions(
    make_node: NodeFactory,
) -> None:
    """Each demotion restarts the state watcher; old ones must unsubscribe."""
    a = await make_node("node_a")
    nc = a.cluster.coordinator.nc
    await asyncio.sleep(0.5)
    before = len(nc._subs)

    for _ in range(5):
        a.cluster.replicator._start_watcher()
        await asyncio.sleep(0.3)
    await asyncio.sleep(0.5)

    assert len(nc._subs) == before
