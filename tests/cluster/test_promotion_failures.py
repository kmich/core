"""A promotion or handover that fails must not leave a node leading, or no one.

Tests against a real NATS JetStream server.
"""

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from homeassistant.cluster.const import LEADER_KEY
from homeassistant.cluster.lease_manager import (
    STANDBY_SETUP_BLOCKED,
    HandoverIncomplete,
    LeaseManager,
)
from homeassistant.cluster.registry_sync import async_load_registries
from homeassistant.cluster.state_replicator import StateReplicator
from homeassistant.cluster.storage_sync import StorageSync
from homeassistant.cluster.ui_config_sync import UiConfigSync
from homeassistant.config_entries import ConfigEntryState
from homeassistant.helpers import area_registry as ar

from .conftest import NodeFactory
from .test_init import wait_for

LOADED = ConfigEntryState.LOADED
NOT_LOADED = ConfigEntryState.NOT_LOADED


@pytest.mark.parametrize(
    "error",
    [OSError("disk"), RuntimeError("boom"), HandoverIncomplete("later")],
    ids=["os-error", "unexpected", "handover-incomplete"],
)
async def test_failed_pre_setup_hook_keeps_the_entry_on_standby(
    make_node: NodeFactory, error: Exception
) -> None:
    """Whatever a pre-setup hook raised, the entry does not start."""
    a = await make_node("node_a")
    leases = a.cluster.leases
    await a.hass.config_entries.async_unload(a.entry.entry_id)
    assert a.entry.state is NOT_LOADED

    async def failing(entry: Any) -> None:
        raise error

    leases.add_before_setup(failing)
    await leases._async_setup_promoted_entry(a.entry)

    assert a.entry.state is NOT_LOADED
    assert a.entry.entry_id in leases._standby
    if not isinstance(error, HandoverIncomplete):
        # Not a quiet wait: the entry says why it does not run here, and the
        # leader counts it as failing so that another node gets a turn.
        assert a.entry.reason == STANDBY_SETUP_BLOCKED
        assert a.entry.entry_id in leases._setup_blocked


async def test_entry_record_that_cannot_be_applied_blocks_the_promotion(
    make_node: NodeFactory,
) -> None:
    """A record that fails for a reason other than its content raises."""
    a = await make_node("node_a")
    storage = a.cluster.storage
    record = {
        "entry": {
            "entry_id": "x",
            "domain": "cluster_dev",
            "created_at": "2026-01-01T00:00:00+00:00",
            "data": {},
            "options": {},
        }
    }
    with patch.object(storage, "_async_apply", side_effect=PermissionError("denied")):
        with pytest.raises(HandoverIncomplete):
            await storage._async_apply_record(record, strict=True)
        # While standing by, a failure is only logged.
        await storage._async_apply_record(record)
    # A failure while applying a valid-looking record may have changed
    # something: even a KeyError from inside it stops the promotion.
    with (
        patch.object(storage, "_async_apply", side_effect=KeyError("deep")),
        pytest.raises(HandoverIncomplete),
    ):
        await storage._async_apply_record(record, strict=True)
    # A record whose content is wrong is skipped, also during a promotion.
    for wrong in ({}, {"entry": {}}, {"entry": {"entry_id": 1}}, "text"):
        await storage._async_apply_record(wrong, strict=True)


async def test_ui_record_that_cannot_be_applied_blocks_the_promotion(
    make_node: NodeFactory,
) -> None:
    """The leader's UI files are applied or the promotion does not go on."""
    a = await make_node("node_a")
    ui: UiConfigSync = a.cluster.storage.ui
    record = b'{"origin": "node_x", "content": "a: 1"}'
    with patch.object(ui, "_async_apply", side_effect=PermissionError("denied")):
        with pytest.raises(HandoverIncomplete):
            await ui._async_apply_raw("ui.file.automations.yaml", record, strict=True)
        await ui._async_apply_raw("ui.file.automations.yaml", record)
    # A record whose content is wrong is skipped, also during a promotion.
    await ui._async_apply_raw(
        "ui.file.automations.yaml", b'{"origin": "node_x", "content": 5}', strict=True
    )
    await ui._async_apply_raw("ui.collection.input_boolean", b"{}", strict=True)


async def test_registry_that_cannot_be_loaded_blocks_the_promotion(
    make_node: NodeFactory, area_registry: ar.AreaRegistry
) -> None:
    """A node does not lead with registries that are half the leader's."""
    a = await make_node("node_a")
    kv = a.cluster.coordinator.kv_storage
    envelope = (
        b'{"origin": "node_x", "content": {"version": %d, "minor_version": %d,'
        b' "data": {"areas": []}}}'
        % (area_registry._store.version, area_registry._store.minor_version)
    )
    await kv.put("store.core.floor_registry", b"")
    await kv.put("store.core.area_registry", envelope)
    with (
        patch.object(
            area_registry.__class__, "async_replace", side_effect=OSError("disk")
        ),
        pytest.raises(HandoverIncomplete),
    ):
        await async_load_registries(a.hass, "node_a", kv, a.cluster.leases)


async def test_forwarded_call_in_flight_blocks_a_clean_release(
    make_node: NodeFactory,
) -> None:
    """A service call another node forwarded is work of the leader.

    While it runs the lease is not released cleanly; when leadership ends it
    gets the unload grace to finish and is then cancelled.
    """
    a = await make_node("node_a")
    router = a.cluster.router
    leases = a.cluster.leases
    started = asyncio.Event()

    async def long_call() -> None:
        started.set()
        await asyncio.Event().wait()

    task = asyncio.create_task(long_call())
    router._rpc_tasks.add(task)
    task.add_done_callback(router._rpc_tasks.discard)
    await started.wait()

    assert router.busy()
    assert not leases._is_quiescent()

    await leases._async_lose_leader()

    assert task.cancelled()
    assert not router.busy()
    assert leases._is_quiescent()


async def test_forwarded_call_that_finishes_in_time_is_not_cancelled(
    make_node: NodeFactory,
) -> None:
    """A call that ends within the grace completes normally."""
    a = await make_node("node_a")
    router = a.cluster.router

    async def short_call() -> str:
        await asyncio.sleep(0.2)
        return "done"

    task = asyncio.create_task(short_call())
    router._rpc_tasks.add(task)
    task.add_done_callback(router._rpc_tasks.discard)

    await a.cluster.leases._async_lose_leader()

    assert task.result() == "done"
    assert not router.busy()


async def test_promotion_that_raises_gives_the_lease_to_another_node(
    make_node: NodeFactory,
) -> None:
    """An exception in the promotion must not leave a held lease and no leader."""
    original = StorageSync.async_before_promote

    async def failing(self: StorageSync, is_handover: bool) -> None:
        if self.node_id == "node_a":
            raise RuntimeError("boom")
        await original(self, is_handover)

    with patch.object(StorageSync, "async_before_promote", failing):
        a = await make_node("node_a")
        b = await make_node("node_b")
        await wait_for(lambda: b.entry.state is LOADED, timeout=30)

    assert b.cluster.leases.is_leader
    assert not a.cluster.leases.is_leader
    assert a.entry.state is NOT_LOADED


async def test_promotion_that_raises_after_entries_started_unloads_them(
    make_node: NodeFactory,
) -> None:
    """Entries that already started are unloaded when the promotion fails."""
    original = StorageSync.async_flush
    failures: list[str] = []

    async def failing(self: StorageSync) -> bool:
        if self.node_id == "node_a" and not failures:
            failures.append("node_a")
            raise RuntimeError("boom")
        return await original(self)

    rec = make_node.recorder
    with patch.object(StorageSync, "async_flush", failing):
        a = await make_node("node_a")
        # The promotion failed once after the entry started: it was unloaded
        # and the lease given up. With nobody else around, the node tries
        # again and then leads.
        await wait_for(
            lambda: a.entry.state is LOADED and a.cluster.leases.is_leader, timeout=30
        )

    assert failures == ["node_a"]
    assert len(rec.times("node_a", "unload")) == 1
    assert len(rec.times("node_a", "setup")) == 2


@pytest.mark.parametrize("where", ["replicator", "entry_loop", "abort_also_fails"])
async def test_promotion_failure_anywhere_ends_with_one_leader(
    make_node: NodeFactory, where: str
) -> None:
    """Wherever a promotion raises, even in the undo, another node can lead."""
    original_replicator = StateReplicator.async_before_promote
    original_setup = LeaseManager._async_setup_promoted_entry
    original_lose = LeaseManager._async_lose_leader
    aborts: list[int] = []

    async def replicator(self: StateReplicator) -> None:
        if self.node_id == "node_a":
            raise RuntimeError("boom")
        await original_replicator(self)

    async def setup(self: LeaseManager, entry: Any) -> None:
        if self.node_id == "node_a":
            raise RuntimeError("boom")
        await original_setup(self, entry)

    async def lose(self: LeaseManager) -> None:
        if self.node_id == "node_a" and not aborts:
            aborts.append(1)
            raise RuntimeError("the undo failed too")
        await original_lose(self)

    patches = {
        "replicator": [
            patch.object(StateReplicator, "async_before_promote", replicator)
        ],
        "entry_loop": [
            patch.object(LeaseManager, "_async_setup_promoted_entry", setup)
        ],
        "abort_also_fails": [
            patch.object(StateReplicator, "async_before_promote", replicator),
            patch.object(LeaseManager, "_async_lose_leader", lose),
        ],
    }[where]
    for p in patches:
        p.start()
    try:
        a = await make_node("node_a")
        b = await make_node("node_b")
        await wait_for(lambda: b.entry.state is LOADED, timeout=40)
    finally:
        for p in patches:
            p.stop()

    assert b.cluster.leases.is_leader
    assert not a.cluster.leases.is_leader
    assert LEADER_KEY not in a.cluster.leases.held
