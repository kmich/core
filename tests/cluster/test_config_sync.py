"""Configuration made on the leader reaches the standby: entries added, removed, disabled or given subentries."""

import asyncio
import json
import time
from types import MappingProxyType
from unittest.mock import patch

import pytest
from pytest_homeassistant.common import (
    MockConfigEntry,
    MockModule,
    MockPlatform,
    mock_integration,
    mock_platform,
)

from homeassistant.cluster.coordinator import ClusterCoordinator
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigEntryDisabler,
    ConfigEntryState,
    ConfigSubentry,
)
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .conftest import (
    DEV_DOMAIN,
    DEV_ENTRY_ID,
    DevRecorder,
    DevSwitch,
    NodeFactory,
    seal_entry_payload,
    start_node,
)
from .test_init import wait_for
from .test_readiness import failing_storage_put

LOADED = ConfigEntryState.LOADED
NOT_LOADED = ConfigEntryState.NOT_LOADED

NEW_ENTRY_ID = "01JCLUSTERDEVENTRYNEW00000"
STANDBY_OF_A = "Cluster standby: running on node node_a"


def new_entry() -> MockConfigEntry:
    """A config entry that only the node it is added to knows."""
    return MockConfigEntry(
        domain=DEV_DOMAIN, entry_id=NEW_ENTRY_ID, title="New", data={"token": "n1"}
    )


def install_with_remove_hook(hass: HomeAssistant, node: str, rec: DevRecorder) -> None:
    """Fake integration that records its ``async_remove_entry`` clean-up."""

    async def setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "setup", dict(entry.data)))
        await hass.config_entries.async_forward_entry_setups(entry, ["switch"])

    async def unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "unload", dict(entry.data)))
        await hass.config_entries.async_unload_platforms(entry, ["switch"])

    async def remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "remove_entry", {}))

    async def switch_setup_entry(
        hass: HomeAssistant,
        entry: ConfigEntry,
        async_add_entities: AddConfigEntryEntitiesCallback,
    ) -> None:
        async_add_entities([DevSwitch(entry, node)])

    mock_integration(
        hass,
        MockModule(
            DEV_DOMAIN,
            async_setup_entry=setup_entry,
            async_unload_entry=unload_entry,
            async_remove_entry=remove_entry,
        ),
    )
    mock_platform(
        hass, f"{DEV_DOMAIN}.switch", MockPlatform(async_setup_entry=switch_setup_entry)
    )
    mock_platform(hass, f"{DEV_DOMAIN}.config_flow", None)


async def test_entry_added_on_the_leader_appears_on_the_standby(
    make_node: NodeFactory,
) -> None:
    """The standby gets the new entry while the leader still leads."""
    rec = make_node.recorder
    a = await make_node("node_a")
    b = await make_node("node_b")
    new = new_entry()
    await a.hass.config_entries.async_add(new)
    await a.hass.async_block_till_done()
    assert new.state is LOADED

    await wait_for(
        lambda: b.hass.config_entries.async_get_entry(NEW_ENTRY_ID) is not None,
        timeout=15,
    )
    replica = b.hass.config_entries.async_get_entry(NEW_ENTRY_ID)
    assert replica is not None
    assert (replica.domain, replica.title) == (DEV_DOMAIN, "New")
    assert dict(replica.data) == {"token": "n1"}
    assert replica.created_at == new.created_at
    await wait_for(lambda: replica.reason == STANDBY_OF_A, timeout=5)
    assert replica.state is NOT_LOADED
    assert [d for _, n, k, d in rec.events if (n, k) == ("node_b", "setup")] == []


async def test_entry_added_before_the_standby_started_exists_there(
    make_node: NodeFactory,
) -> None:
    """A node that starts later learns the entries it missed."""
    a = await make_node("node_a")
    await a.hass.config_entries.async_add(new_entry())
    await a.hass.async_block_till_done()
    await asyncio.sleep(2)

    b = await make_node("node_b")

    await wait_for(
        lambda: b.hass.config_entries.async_get_entry(NEW_ENTRY_ID) is not None,
        timeout=15,
    )
    replica = b.hass.config_entries.async_get_entry(NEW_ENTRY_ID)
    assert replica is not None
    assert replica.state is NOT_LOADED


async def test_entry_removed_on_the_leader_is_removed_on_the_standby(
    make_node: NodeFactory,
) -> None:
    """The standby drops the entry and does not run the clean-up again."""
    rec = make_node.recorder
    a = await make_node("node_a", start=False)
    b = await make_node("node_b", start=False)
    for node in (a, b):
        install_with_remove_hook(node.hass, node.name, rec)
        await start_node(node, make_node.url)
    assert (a.entry.state, b.entry.state) == (LOADED, NOT_LOADED)

    await a.hass.config_entries.async_remove(DEV_ENTRY_ID)

    await wait_for(
        lambda: b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is None,
        timeout=15,
    )
    # The integration's clean-up (cloud resources, webhooks) ran once.
    assert [n for _, n, k, _ in rec.events if k == "remove_entry"] == ["node_a"]


async def test_entry_removed_before_the_standby_started_is_removed_there(
    make_node: NodeFactory,
) -> None:
    """A removal is remembered for a node that was down."""
    a = await make_node("node_a")
    await a.hass.config_entries.async_remove(DEV_ENTRY_ID)
    await asyncio.sleep(2)

    b = await make_node("node_b")

    await wait_for(
        lambda: b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is None,
        timeout=15,
    )


async def test_entry_disabled_on_the_leader_is_disabled_on_the_standby(
    make_node: NodeFactory,
) -> None:
    """Disabling and enabling an entry follows the leader."""
    a = await make_node("node_a")
    b = await make_node("node_b")

    await a.hass.config_entries.async_set_disabled_by(
        DEV_ENTRY_ID, ConfigEntryDisabler.USER
    )
    await wait_for(lambda: b.entry.disabled_by is ConfigEntryDisabler.USER, timeout=15)

    await a.hass.config_entries.async_set_disabled_by(DEV_ENTRY_ID, None)
    await wait_for(lambda: b.entry.disabled_by is None, timeout=15)
    await wait_for(lambda: a.entry.state is LOADED, timeout=10)
    await asyncio.sleep(1)
    assert b.entry.state is NOT_LOADED


async def test_subentry_added_on_the_leader_reaches_the_standby(
    make_node: NodeFactory,
) -> None:
    """Subentries added, changed and removed on the leader follow."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    subentry = ConfigSubentry(
        data=MappingProxyType({"area": "kitchen"}),
        subentry_type="zone",
        title="Kitchen",
        unique_id="zone-kitchen",
    )
    subentry_id = subentry.subentry_id

    a.hass.config_entries.async_add_subentry(a.entry, subentry)
    await wait_for(lambda: subentry_id in b.entry.subentries, timeout=15)
    assert b.entry.subentries[subentry_id].as_dict() == subentry.as_dict()

    a.hass.config_entries.async_update_subentry(a.entry, subentry, title="Kitchen 2")
    await wait_for(
        lambda: b.entry.subentries[subentry_id].title == "Kitchen 2", timeout=15
    )

    a.hass.config_entries.async_remove_subentry(a.entry, subentry_id)
    await wait_for(lambda: subentry_id not in b.entry.subentries, timeout=15)


async def test_leader_does_not_apply_records_it_holds(make_node: NodeFactory) -> None:
    """A node that keeps the lease never pulls; a standby applies the same."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    record = a.entry.as_dict()
    record["title"] = "Foreign"
    kv = a.cluster.coordinator.kv_storage
    assert kv is not None
    await kv.put(
        f"entry.{DEV_ENTRY_ID}",
        seal_entry_payload(
            f"entry.{DEV_ENTRY_ID}",
            {"origin": "node_x", "ts": time.time(), "entry": record},
        ),
    )

    await wait_for(lambda: b.entry.title == "Foreign", timeout=15)
    await asyncio.sleep(1)
    assert a.entry.title != "Foreign"
    assert a.entry.state is LOADED


async def test_failed_tombstone_makes_the_flush_fail(make_node: NodeFactory) -> None:
    """A removal that could not be published is not flushed over."""
    a = await make_node("node_a")
    storage = a.cluster.storage
    kv = a.cluster.coordinator.kv_storage
    assert kv is not None
    calls: list[str] = []
    failing = failing_storage_put(calls)
    real_put = ClusterCoordinator.async_fenced_put

    async def tombstone_fails(self: ClusterCoordinator, key: str, value: bytes) -> int:
        # The announcement before the clean-up goes through; the tombstone
        # after it does not.
        if b'"removed":true' not in value:
            return await real_put(self, key, value)
        return await failing(self, key, value)

    with patch.object(ClusterCoordinator, "async_fenced_put", tombstone_fails):
        await a.hass.config_entries.async_remove(DEV_ENTRY_ID)
        await asyncio.sleep(0.5)
        assert f"entry.{DEV_ENTRY_ID}" in calls
        with pytest.raises(HomeAssistantError):
            await asyncio.wait_for(storage.async_flush(), 15)

    await asyncio.wait_for(storage.async_flush(), 15)
    tombstone = json.loads((await kv.get(f"entry.{DEV_ENTRY_ID}")).value)
    assert tombstone["removed"] is True
    assert tombstone["entry_id"] == DEV_ENTRY_ID
