"""Repair flows on a cluster instance that does not run the entry."""

from unittest.mock import AsyncMock, patch

from pytest_homeassistant.common import MockConfigEntry

from homeassistant.components.homeassistant.repairs import (
    IntegrationNotFoundFlow,
    OrphanedConfigEntryFlow,
)
from homeassistant.config_entries import OperationNotAllowed
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType


async def test_orphaned_entry_cannot_be_removed_on_a_standby(
    hass: HomeAssistant,
) -> None:
    """A placement that refuses the removal gives an abort, not an exception."""
    entry = MockConfigEntry(domain="gone")
    entry.add_to_hass(hass)
    flow = OrphanedConfigEntryFlow({"entry_id": entry.entry_id, "domain": "gone"})
    flow.hass = hass

    with patch.object(
        hass.config_entries,
        "async_remove",
        AsyncMock(side_effect=OperationNotAllowed("runs elsewhere")),
    ):
        result = await flow.async_step_confirm()

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "cluster_standby"


async def test_missing_integration_cannot_be_removed_on_a_standby(
    hass: HomeAssistant,
) -> None:
    """The same for the repair of an integration that cannot be found."""
    MockConfigEntry(domain="gone").add_to_hass(hass)
    flow = IntegrationNotFoundFlow({"domain": "gone"})
    flow.hass = hass

    with patch.object(
        hass.config_entries,
        "async_remove",
        AsyncMock(side_effect=OperationNotAllowed("runs elsewhere")),
    ):
        result = await flow.async_step_confirm()

    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "cluster_standby"
