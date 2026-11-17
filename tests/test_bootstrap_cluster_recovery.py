"""Forced recovery of a configured cluster node."""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from pytest_homeassistant.common import (
    MockConfigEntry,
    MockModule,
    mock_config_flow,
    mock_integration,
    mock_platform,
)

from homeassistant import bootstrap, runner
from homeassistant.config_entries import ConfigEntryState, ConfigFlow


@pytest.fixture(autouse=True)
def disable_block_async_io(disable_block_async_io):
    """Disable the loop protection from block_async_io after each test.

    Bootstrap switches it on for the whole process; without this every later
    test that sleeps (the watchdog tests) is refused.
    """


@pytest.fixture(autouse=True)
def disable_block_shared_serial(disable_block_shared_serial: None) -> None:
    """Restore pyserial after each test."""


@pytest.mark.usefixtures("hass_storage")
async def test_forced_recovery_without_marker_keeps_cluster_entry_unloaded(
    tmp_path: Path,
) -> None:
    """Recovery of a new member must keep cluster placement closed."""
    (tmp_path / "configuration.yaml").write_text(
        "cluster:\n  node_id: node_a\n  nats_url: nats://127.0.0.1:4222\n",
        encoding="utf-8",
    )
    with (
        patch.object(bootstrap, "async_enable_logging"),
        patch("homeassistant.util.package.is_installed", return_value=True),
    ):
        hass = await bootstrap.async_setup_hass(
            runner.RuntimeConfig(
                config_dir=str(tmp_path),
                verbose=False,
                log_rotate_days=10,
                log_file="",
                log_no_color=False,
                skip_pip=True,
                recovery_mode=True,
            ),
        )

    assert hass is not None
    try:
        assert hass.config.recovery_mode
        mock_integration(
            hass, MockModule("hub_like", async_setup_entry=AsyncMock(return_value=None))
        )
        mock_platform(hass, "hub_like.config_flow", None)
        entry = MockConfigEntry(domain="hub_like")
        entry.add_to_hass(hass)
        with mock_config_flow("hub_like", ConfigFlow):
            await hass.config_entries.async_setup(entry.entry_id)
        assert entry.state is ConfigEntryState.NOT_LOADED
    finally:
        await hass.async_stop(force=True)


async def test_recovery_mode_keeps_the_membership_marker(
    tmp_path: Path, hass_storage: dict[str, Any]
) -> None:
    """A configuration that cannot be read must not make the node forget its cluster.

    Recovery mode starts with a stub configuration that has no ``cluster`` key;
    that says nothing about the membership, and the marker is what the next
    recovery relies on.
    """
    (tmp_path / "configuration.yaml").write_text("cluster: [\n", encoding="utf-8")
    hass_storage["cluster"] = {
        "version": 1,
        "minor_version": 1,
        "key": "cluster",
        "data": {"member": True},
    }
    with (
        patch.object(bootstrap, "async_enable_logging"),
        patch("homeassistant.util.package.is_installed", return_value=True),
    ):
        hass = await bootstrap.async_setup_hass(
            runner.RuntimeConfig(
                config_dir=str(tmp_path),
                verbose=False,
                log_rotate_days=10,
                log_file="",
                log_no_color=False,
                skip_pip=True,
            ),
        )

    assert hass is not None
    try:
        assert hass.config.recovery_mode
        assert hass_storage["cluster"]["data"] == {"member": True}
    finally:
        await hass.async_stop(force=True)
