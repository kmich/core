"""Supervisor discovery removal on a cluster instance that does not run the entry."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from aiohasupervisor import SupervisorError
from pytest_homeassistant.common import MockConfigEntry

from homeassistant import config_entries
from homeassistant.components.hassio.discovery import HassIODiscovery
from homeassistant.core import HomeAssistant


async def test_discovery_removal_is_left_to_the_instance_that_runs_the_entry(
    hass: HomeAssistant,
) -> None:
    """A placement that refuses the removal does not make the push fail."""
    uuid = str(uuid4())
    MockConfigEntry(
        domain="svc", source=config_entries.SOURCE_HASSIO, unique_id=uuid
    ).add_to_hass(hass)
    client = MagicMock()
    client.discovery.get = AsyncMock(side_effect=SupervisorError)
    with patch(
        "homeassistant.components.hassio.discovery.get_supervisor_client",
        return_value=client,
    ):
        view = HassIODiscovery(hass)

    remove = AsyncMock(side_effect=config_entries.OperationNotAllowed("runs elsewhere"))
    with patch.object(hass.config_entries, "async_remove", remove):
        await view.async_process_del({"service": "svc", "uuid": uuid})

    remove.assert_awaited_once()
