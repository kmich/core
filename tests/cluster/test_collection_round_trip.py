"""Every UI helper collection survives ``async_dump`` and ``async_replace``.

A standby gets the leader's helpers by replacing its collection with the
leader's dump. A domain that loses a field in that round trip would run
different helpers after a failover. Fields are filled in as far as the
domain allows; the comparison is of the dump and of what would be saved.
"""

from typing import Any

import pytest
from pytest_homeassistant.common import async_test_home_assistant

from homeassistant.cluster.ui_config_sync import UI_HELPER_DOMAINS
from homeassistant.core import HomeAssistant
from homeassistant.helpers.collection import async_get_storage_collection
from homeassistant.setup import async_setup_component

# One item per domain with every optional field the domain accepts.
ITEMS: dict[str, dict[str, Any]] = {
    "counter": {
        "name": "c",
        "icon": "mdi:counter",
        "initial": 3,
        "minimum": 0,
        "maximum": 10,
        "restore": False,
        "step": 2,
    },
    "input_boolean": {"name": "b", "icon": "mdi:toggle-switch", "initial": True},
    "input_button": {"name": "btn", "icon": "mdi:gesture-tap-button"},
    "input_datetime": {
        "name": "d",
        "icon": "mdi:calendar",
        "has_date": True,
        "has_time": True,
        "initial": "2026-01-01 10:00:00",
    },
    "input_number": {
        "name": "n",
        "icon": "mdi:numeric",
        "min": 0,
        "max": 100,
        "step": 5,
        "initial": 10,
        "mode": "slider",
        "unit_of_measurement": "kW",
    },
    "input_select": {
        "name": "s",
        "icon": "mdi:format-list-bulleted",
        "options": ["a", "b"],
        "initial": "a",
    },
    "input_text": {
        "name": "t",
        "icon": "mdi:text",
        "min": 1,
        "max": 20,
        "initial": "hi",
        "mode": "text",
        "pattern": "[a-z]+",
    },
    "person": {
        "name": "p",
        "user_id": None,
        "device_trackers": ["device_tracker.phone"],
        "picture": "/local/p.png",
    },
    "schedule": {
        "name": "sch",
        "icon": "mdi:calendar-clock",
        "monday": [{"from": "07:00:00", "to": "08:00:00", "data": {"k": 1}}],
        "sunday": [{"from": "09:00:00", "to": "10:30:00"}],
    },
    "tag": {"tag_id": "abc-123", "name": "front door tag"},
    "timer": {
        "name": "tm",
        "icon": "mdi:timer",
        "duration": "0:05:00",
        "restore": True,
    },
    "zone": {
        "name": "z",
        "icon": "mdi:map-marker",
        "latitude": 1.5,
        "longitude": 2.5,
        "radius": 120,
        "passive": True,
    },
}


@pytest.fixture
def expected_lingering_timers(request: pytest.FixtureRequest) -> bool:
    """A schedule keeps a timer for its next event on the second instance."""
    callspec = getattr(request.node, "callspec", None)
    return callspec is not None and callspec.params.get("domain") == "schedule"


def test_every_domain_has_an_item() -> None:
    """A new helper domain must be added here too."""
    assert set(ITEMS) == set(UI_HELPER_DOMAINS)


@pytest.mark.usefixtures("hass_storage")
@pytest.mark.parametrize("domain", sorted(UI_HELPER_DOMAINS))
async def test_collection_round_trips(hass: HomeAssistant, domain: str) -> None:
    """A second instance ends with the same items, dump and saved data."""
    assert await async_setup_component(hass, domain, {})
    leader = async_get_storage_collection(hass, domain)
    assert leader is not None, f"{domain} has no storage collection"
    await leader.async_create_item(ITEMS[domain])
    dump = leader.async_dump()
    assert dump["items"], f"{domain}: the item was not created"

    async with async_test_home_assistant() as other:
        assert await async_setup_component(other, domain, {})
        standby = async_get_storage_collection(other, domain)
        assert standby is not None
        await standby.async_replace(dump)
        await other.async_block_till_done()

        assert standby.async_dump() == dump
        assert standby._base_data_to_save() == leader._base_data_to_save()
        # Replacing again with the same dump changes nothing.
        await standby.async_replace(dump)
        assert standby.async_dump() == dump
        await other.async_block_till_done()
        await other.async_stop(force=True)
