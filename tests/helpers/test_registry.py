"""Tests for the registry."""

from typing import Any

from freezegun.api import FrozenDateTimeFactory
import pytest
from pytest_homeassistant.common import MockConfigEntry, async_fire_time_changed

from homeassistant.core import CoreState, HomeAssistant
from homeassistant.helpers import (
    area_registry as ar,
    device_registry as dr,
    entity_registry as er,
    storage,
)
from homeassistant.helpers.registry import SAVE_DELAY, SAVE_DELAY_LONG, BaseRegistry


class SampleRegistry(BaseRegistry):
    """Class to hold a registry of X."""

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the registry."""
        self.hass = hass
        self._store = storage.Store(hass, 1, "test")
        self.save_calls = 0

    async def _async_load(self) -> None:
        """Load the registry."""

    def _data_to_save(self) -> dict[str, Any]:
        """Return data of registry to save."""
        self.save_calls += 1
        return {}


@pytest.mark.parametrize(
    "long_delay_state",
    [
        CoreState.not_running,
        CoreState.starting,
        CoreState.stopped,
        CoreState.final_write,
    ],
)
async def test_async_schedule_save(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    long_delay_state: CoreState,
    hass_storage: dict[str, Any],
) -> None:
    """Test saving the registry.

    If CoreState is not running, it should save with long delay.

    Storage will always save at final write if there is a
    write pending so we should not schedule a save in that case.
    """
    registry = SampleRegistry(hass)
    hass.set_state(long_delay_state)

    registry.async_schedule_save()
    freezer.tick(SAVE_DELAY)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert registry.save_calls == 0

    freezer.tick(SAVE_DELAY_LONG)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert registry.save_calls == 1

    hass.set_state(CoreState.running)
    registry.async_schedule_save()
    freezer.tick(SAVE_DELAY)
    async_fire_time_changed(hass)
    await hass.async_block_till_done()
    assert registry.save_calls == 2


async def test_replace_keeps_ids_and_drops_what_is_not_in_the_data(
    hass: HomeAssistant,
) -> None:
    """Test that replacing a registry loads the data as a store file would."""
    registry = ar.async_get(hass)
    kitchen = registry.async_create("Kitchen")
    garage = registry.async_create("Garage")
    data = registry.async_dump()
    data["areas"] = [area for area in data["areas"] if area["id"] == kitchen.id]
    events: list[Any] = []
    hass.bus.async_listen(ar.EVENT_AREA_REGISTRY_UPDATED, events.append)
    listeners = dict(hass.bus.async_listeners())

    await registry.async_replace(data)
    await hass.async_block_till_done()

    assert [area.id for area in registry.async_list_areas()] == [kitchen.id]
    assert registry.async_get_area(garage.id) is None
    assert registry.async_get_area(kitchen.id) == kitchen
    # No event, and the one-time setup did not run again.
    assert events == []
    assert dict(hass.bus.async_listeners()) == listeners
    assert registry._replacement is None

    # A second replace works.
    await registry.async_replace(registry.async_dump())
    assert [area.id for area in registry.async_list_areas()] == [kitchen.id]


async def test_replace_entity_registry(hass: HomeAssistant) -> None:
    """Test that the entity registry keeps ids on replace and still refuses a load."""
    registry = er.async_get(hass)
    entry = registry.async_get_or_create("light", "test", "1234")
    data = registry.async_dump()
    registry.async_remove(entry.entity_id)
    assert registry.async_get(entry.entity_id) is None
    listeners = dict(hass.bus.async_listeners())

    await registry.async_replace(data)

    restored = registry.async_get(entry.entity_id)
    assert restored is not None
    assert restored.id == entry.id
    assert dict(hass.bus.async_listeners()) == listeners
    await registry.async_replace(registry.async_dump())
    assert registry.async_get(entry.entity_id) is not None

    with pytest.raises(RuntimeError, match="already loaded"):
        await registry.async_load()


async def test_replace_device_registry(hass: HomeAssistant) -> None:
    """Test that the device registry keeps ids on replace and still refuses a load."""
    config_entry = MockConfigEntry(domain="test")
    config_entry.add_to_hass(hass)
    registry = dr.async_get(hass)
    device = registry.async_get_or_create(
        config_entry_id=config_entry.entry_id, identifiers={("test", "1")}
    )
    data = registry.async_dump()
    registry.async_remove_device(device.id)
    assert registry.async_get(device.id) is None
    listeners = dict(hass.bus.async_listeners())

    await registry.async_replace(data)

    restored = registry.async_get(device.id)
    assert restored is not None
    assert restored.identifiers == {("test", "1")}
    assert dict(hass.bus.async_listeners()) == listeners
    await registry.async_replace(registry.async_dump())
    assert registry.async_get(device.id) is not None

    with pytest.raises(RuntimeError, match="already loaded"):
        await registry.async_load()
