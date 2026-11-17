"""Shutdown, teardown and removal clean-up of a cluster node, against JetStream."""

import asyncio
from unittest.mock import patch

from pytest_homeassistant.common import (
    MockModule,
    MockPlatform,
    mock_integration,
    mock_platform,
)

from homeassistant.cluster.const import LEADER_KEY
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.setup import async_setup_component

from .conftest import DEV_ENTRY_ID, HardExit, NodeFactory, start_node
from .test_init import wait_for
from .test_yaml_platforms import PROBE, SENSOR_CONFIG, YAML_DOMAIN, YamlProbe


class FailingRemovalProbe(YamlProbe):
    """A platform entity whose cleanup fails."""

    async def async_will_remove_from_hass(self) -> None:
        """Keep the entity active when its cleanup fails."""
        raise RuntimeError("YAML probe cleanup failed")


async def test_failed_yaml_teardown_keeps_watchdog_armed(
    make_node: NodeFactory, hard_exit: HardExit
) -> None:
    """A failed YAML teardown must leave the watchdog protecting handover."""
    a = await make_node("node_a")

    async def setup_platform(
        hass: HomeAssistant,
        config: ConfigType,
        async_add_entities: AddEntitiesCallback,
        discovery_info: DiscoveryInfoType | None = None,
    ) -> None:
        async_add_entities([FailingRemovalProbe("node_a")])

    mock_integration(a.hass, MockModule(YAML_DOMAIN))
    mock_platform(
        a.hass,
        f"{YAML_DOMAIN}.sensor",
        MockPlatform(async_setup_platform=setup_platform),
    )
    assert await async_setup_component(a.hass, "sensor", SENSOR_CONFIG)
    await a.hass.async_block_till_done()
    assert a.hass.states.get(PROBE) is not None

    leases = a.cluster.leases
    await leases.async_prepare_shutdown()
    await leases.async_stop()

    assert a.hass.states.get(PROBE) is not None
    assert not hard_exit.times
    assert leases._watchdog.armed, "watchdog disarmed despite failed entity teardown"


async def test_removal_callback_blocks_clean_release(make_node: NodeFactory) -> None:
    """Tracked work must include cleanup for an entry removed from Core's map."""
    a = await make_node("node_a")
    cleanup_started = asyncio.Event()
    cleanup_finish = asyncio.Event()
    original_remove = ConfigEntry.async_remove

    async def remove(entry: ConfigEntry, hass: HomeAssistant) -> None:
        cleanup_started.set()
        await cleanup_finish.wait()
        await original_remove(entry, hass)

    with patch.object(ConfigEntry, "async_remove", remove):
        removal = asyncio.create_task(a.hass.config_entries.async_remove(DEV_ENTRY_ID))
        try:
            async with asyncio.timeout(5):
                await cleanup_started.wait()
            assert a.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is None
            await a.cluster.leases.async_prepare_shutdown()
            await a.cluster.leases.async_stop()
            kv = a.cluster.coordinator.kv_leases
            assert kv is not None
            record = await kv.get(LEADER_KEY)
            assert b"node_a" in record.value
        finally:
            cleanup_finish.set()
            await removal


async def test_unseen_entry_removal_preserves_cleanup_data(
    make_node: NodeFactory,
) -> None:
    """A standby with no local copy must still be able to do announced cleanup."""
    a = await make_node("node_a")
    b = await make_node("node_b", start=False)
    await b.hass.config_entries._async_remove_replica(DEV_ENTRY_ID)
    await a.cluster.storage.async_publish_removing(a.entry)
    await start_node(b, make_node.url)
    assert b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is None

    await a.hass.async_stop()
    await wait_for(lambda: b.cluster.leases.is_leader, timeout=15)

    assert make_node.recorder.times("node_b", "cleanup"), (
        "the promoting standby had no local config entry to clean up"
    )


async def test_resigned_node_refuses_new_entry_setup(make_node: NodeFactory) -> None:
    """The shutdown job must close placement before it finishes unloading entries."""
    a = await make_node("node_a")
    await a.cluster.leases.async_prepare_shutdown()
    assert a.entry.state is ConfigEntryState.NOT_LOADED

    assert not await a.hass.config_entries.async_setup(DEV_ENTRY_ID), (
        "clustered entry was set up after the node resigned"
    )
