"""Tests for the cluster role helper."""

import asyncio
from collections.abc import Callable
import time
from typing import Any

import pytest
from pytest_homeassistant.common import MockConfigEntry, MockModule, mock_integration

from homeassistant.core import HomeAssistant
from homeassistant.helpers import cluster


async def test_leader_without_cluster(hass: HomeAssistant) -> None:
    """A single instance is the leader, and not clustered."""
    assert cluster.async_is_leader(hass)
    cluster.async_track_leadership(hass, lambda leader: None)
    assert not cluster.async_is_clustered(hass)
    cluster.async_set_leader(hass, True)
    assert cluster.async_is_clustered(hass)


async def test_leadership_changes_are_tracked(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Listeners hear every change, once; a failing one does not stop others."""
    changes: list[bool] = []

    def _raise(leader: bool) -> None:
        raise RuntimeError("listener failed")

    cluster.async_track_leadership(hass, _raise)
    remove = cluster.async_track_leadership(hass, changes.append)

    cluster.async_set_leader(hass, False)
    cluster.async_set_leader(hass, False)
    assert not cluster.async_is_leader(hass)
    cluster.async_set_leader(hass, True)
    assert changes == [False, True]
    assert "Error in cluster leadership listener" in caplog.text

    remove()
    cluster.async_set_leader(hass, False)
    assert changes == [False, True]


async def test_leader_deadline(hass: HomeAssistant) -> None:
    """A leader whose deadline passed is no leader, without any listener call."""
    changes: list[bool] = []
    cluster.async_track_leadership(hass, changes.append)
    cluster.async_set_leader(hass, True)
    assert cluster.async_is_leader(hass)

    cluster.async_set_leader_deadline(hass, time.monotonic() + 60)
    assert cluster.async_is_leader(hass)

    cluster.async_set_leader_deadline(hass, time.monotonic() - 0.01)
    assert not cluster.async_is_leader(hass)
    assert changes == []

    cluster.async_set_leader_deadline(hass, time.monotonic() + 60)
    assert cluster.async_is_leader(hass)
    cluster.async_set_leader_deadline(hass, None)
    assert cluster.async_is_leader(hass)


async def test_leader_deadline_does_not_make_a_follower_leader(
    hass: HomeAssistant,
) -> None:
    """A deadline in the future never grants leadership."""
    cluster.async_set_leader(hass, False)
    cluster.async_set_leader_deadline(hass, time.monotonic() + 60)
    assert not cluster.async_is_leader(hass)


async def test_deadline_without_cluster_integration(hass: HomeAssistant) -> None:
    """Without a role, a single instance stays the leader."""
    assert cluster.DATA_CLUSTER_ROLE not in hass.data
    assert cluster.async_is_leader(hass)


@pytest.mark.parametrize(
    ("domain", "integration_type", "every_instance"),
    [
        pytest.param("backup", "service", True, id="every_instance_domain"),
        pytest.param("go2rtc_like", "system", True, id="system_integration"),
        pytest.param("hue_like", "hub", False, id="device_integration"),
    ],
)
async def test_entry_runs_on_every_instance(
    hass: HomeAssistant, domain: str, integration_type: str, every_instance: bool
) -> None:
    """Per-instance infrastructure is not placed on one instance."""
    mock_integration(
        hass,
        MockModule(domain, partial_manifest={"integration_type": integration_type}),
    )
    entry = MockConfigEntry(domain=domain)
    assert cluster.async_entry_runs_on_every_instance(hass, entry) is every_instance


async def test_fail_closed_placement(hass: HomeAssistant) -> None:
    """Without a working cluster, clustered entries stay unloaded."""
    mock_integration(hass, MockModule("hue_like"))
    placement = cluster.FailClosedPlacement(hass)
    entry = MockConfigEntry(domain="hue_like")
    assert (
        await placement.async_place_entry(entry)
        == "The cluster integration is not set up"
    )
    assert placement.async_refuse_removal(entry) is not None
    backup = MockConfigEntry(domain="backup")
    assert await placement.async_place_entry(backup) is None
    assert placement.async_refuse_removal(backup) is None


async def test_fail_closed_until_a_cluster_takes_over(hass: HomeAssistant) -> None:
    """Bootstrap's fail-closed placement is replaced by the cluster's."""
    assert not cluster.async_is_clustered(hass)
    cluster.async_install_fail_closed(hass)
    cluster.async_install_fail_closed(hass)  # once only
    assert cluster.async_is_clustered(hass)
    assert not cluster.async_is_leader(hass)
    assert isinstance(
        hass.config_entries.async_get_placement(), cluster.FailClosedPlacement
    )

    placement = cluster.FailClosedPlacement(hass)
    remove = cluster.async_take_over_placement(hass, placement)
    assert hass.config_entries.async_get_placement() is placement
    assert not cluster.async_is_leader(hass)
    remove()
    assert hass.config_entries.async_get_placement() is None


async def test_transitions_are_tracked_until_done(hass: HomeAssistant) -> None:
    """Work a demotion started counts until it finished, also work added late."""
    assert not cluster.async_transitions_pending(hass)
    await cluster.async_wait_transitions(hass)  # nothing to wait for

    first: asyncio.Future[Any] = hass.loop.create_future()
    second: asyncio.Future[Any] = hass.loop.create_future()
    done: asyncio.Future[Any] = hass.loop.create_future()
    done.set_result(None)
    cluster.async_add_transition_task(hass, done)
    assert not cluster.async_transitions_pending(hass)
    cluster.async_add_transition_task(hass, first)
    assert cluster.async_transitions_pending(hass)

    waiter = hass.async_create_task(cluster.async_wait_transitions(hass))
    await asyncio.sleep(0)
    cluster.async_add_transition_task(hass, second)
    first.set_result(None)
    await asyncio.sleep(0)
    assert not waiter.done()
    second.set_result(None)
    await waiter
    assert not cluster.async_transitions_pending(hass)
    assert cluster.async_transition_failures(hass) == []


async def test_work_that_failed_before_it_was_added_is_a_failure(
    hass: HomeAssistant,
) -> None:
    """An eagerly started task can fail before anybody tracks it."""
    cluster.async_set_leader(hass, True)
    work: asyncio.Future[Any] = hass.loop.create_future()
    work.set_exception(RuntimeError("stopped halfway"))

    cluster.async_add_transition_task(hass, work, "stopping the thing")

    assert cluster.async_transitions_pending(hass)


@pytest.mark.parametrize(
    "finish",
    [
        pytest.param(lambda work: work.set_exception(RuntimeError("x")), id="raised"),
        pytest.param(lambda work: work.cancel(), id="cancelled"),
    ],
)
async def test_failed_transition_stays_pending(
    hass: HomeAssistant, finish: Callable[[asyncio.Future[Any]], object]
) -> None:
    """Work that did not finish what it stopped is never taken as done."""
    cluster.async_set_leader(hass, True)
    work: asyncio.Future[Any] = hass.loop.create_future()
    cluster.async_add_transition_task(hass, work, "stopping the thing")

    finish(work)
    assert not await cluster.async_wait_transitions(hass)

    assert cluster.async_transitions_pending(hass)
    [failure] = cluster.async_transition_failures(hass)
    assert failure.startswith("stopping the thing")


async def test_wait_transitions_times_out(hass: HomeAssistant) -> None:
    """Work that never ends does not hold up the caller beyond the timeout."""
    cluster.async_add_transition_task(hass, hass.loop.create_future())

    assert not await cluster.async_wait_transitions(hass, timeout=0.01)
    assert cluster.async_transitions_pending(hass)


async def test_tracked_work_counts_until_it_ends(hass: HomeAssistant) -> None:
    """Tracked work, such as a removal's clean-up, is pending while it runs."""
    cluster.async_set_leader(hass, True)
    with cluster.async_track_work(hass, "clean-up"):
        assert cluster.async_transitions_pending(hass)
    assert not cluster.async_transitions_pending(hass)

    with pytest.raises(RuntimeError), cluster.async_track_work(hass, "clean-up"):
        raise RuntimeError("boom")
    assert cluster.async_transition_failures(hass) == [
        "clean-up failed: RuntimeError('boom')"
    ]


async def test_tracked_work_is_free_without_a_cluster(hass: HomeAssistant) -> None:
    """Without a cluster nothing is tracked."""
    with cluster.async_track_work(hass, "clean-up"):
        assert not cluster.async_transitions_pending(hass)
    cluster.async_record_failure(hass, "ignored")
    assert not cluster.async_transitions_pending(hass)


@pytest.mark.parametrize(
    ("domain", "integration_type", "configured", "every_instance"),
    [
        pytest.param("backup", None, (), True, id="every_instance_domain"),
        pytest.param("go2rtc_like", "system", (), True, id="system_integration"),
        pytest.param("hue_like", "hub", (), False, id="device_integration"),
        pytest.param("hue_like", "hub", ("hue_like",), True, id="configured"),
        pytest.param("not_loaded", None, (), False, id="not_loaded"),
    ],
)
async def test_domain_runs_on_every_instance(
    hass: HomeAssistant,
    domain: str,
    integration_type: str | None,
    configured: tuple[str, ...],
    every_instance: bool,
) -> None:
    """A domain runs on every instance if it is listed, configured or a system one."""
    if integration_type is not None:
        mock_integration(
            hass,
            MockModule(domain, partial_manifest={"integration_type": integration_type}),
        )
    cluster.async_set_every_instance_domains(hass, configured)
    assert cluster.async_domain_runs_on_every_instance(hass, domain) is every_instance


async def test_listener_that_raises_on_demotion_is_a_failure(
    hass: HomeAssistant,
) -> None:
    """A listener that failed to stop its work keeps the instance un-quiescent."""

    def broken(leader: bool) -> None:
        raise RuntimeError("could not stop")

    cluster.async_set_leader(hass, True)
    cluster.async_track_leadership(hass, broken)

    cluster.async_set_leader(hass, True)
    assert not cluster.async_transitions_pending(hass)
    cluster.async_set_leader(hass, False)

    assert cluster.async_transitions_pending(hass)
