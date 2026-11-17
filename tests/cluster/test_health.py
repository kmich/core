"""The leader endpoint: 200 on the leader, 503 elsewhere, without authentication."""

import asyncio
from http import HTTPStatus
from typing import Any
from unittest.mock import Mock, patch

from pytest_homeassistant.typing import ClientSessionGenerator

from homeassistant.cluster.health import LEADER_URL, ClusterLeaderView
from homeassistant.config_entries import ConfigEntryState
from homeassistant.setup import async_setup_component

from .conftest import NodeFactory
from .test_init import wait_for


async def test_leader_answers_200_and_standby_503(
    make_node: NodeFactory, hass_client_no_auth: ClientSessionGenerator
) -> None:
    """Without logging in, a proxy can tell which node leads; the answer follows it."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    assert await async_setup_component(a.hass, "http", {})
    await a.hass.async_block_till_done()
    client = await hass_client_no_auth()

    resp = await client.get(LEADER_URL)
    assert resp.status == HTTPStatus.OK
    assert await resp.json() == {"leader": True}
    assert resp.headers["Cache-Control"] == "no-store"
    head = await client.head(LEADER_URL)
    assert head.status == HTTPStatus.OK

    standby = ClusterLeaderView(b.cluster)
    assert (await standby.get(Mock())).status == HTTPStatus.SERVICE_UNAVAILABLE
    assert (await standby.head(Mock())).status == HTTPStatus.SERVICE_UNAVAILABLE

    # A leader that shuts down answers 503 at once, before it lets go of
    # the lease; the other node answers 200 once it leads.
    a.cluster.async_resign()
    resp = await client.get(LEADER_URL)
    assert resp.status == HTTPStatus.SERVICE_UNAVAILABLE
    assert await resp.json() == {"leader": False}
    await a.cluster.async_release()
    await wait_for(lambda: b.entry.state is ConfigEntryState.LOADED, timeout=15)
    assert (await standby.get(Mock())).status == HTTPStatus.OK


async def test_new_leader_answers_503_until_its_entries_are_set_up(
    make_node: NodeFactory,
) -> None:
    """A promotion sets up entries one after another: no traffic before that."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    view = ClusterLeaderView(b.cluster)
    leases = b.cluster.leases
    real_setup = leases._async_setup_promoted_entry
    proceed = asyncio.Event()
    reached = asyncio.Event()

    async def _slow_setup(entry: Any) -> None:
        reached.set()
        await proceed.wait()  # a slow integration
        await real_setup(entry)

    with patch.object(leases, "_async_setup_promoted_entry", _slow_setup):
        await a.cluster.async_release()
        await asyncio.wait_for(reached.wait(), 15)
        assert leases.leads  # it acts as leader: its entries may set up
        assert (await view.get(Mock())).status == HTTPStatus.SERVICE_UNAVAILABLE
        proceed.set()
        await wait_for(lambda: b.entry.state is ConfigEntryState.LOADED, timeout=15)
    await wait_for(lambda: b.cluster.serves_as_leader, timeout=5)
    assert (await view.get(Mock())).status == HTTPStatus.OK


async def test_leader_answers_503_while_a_waiting_entry_still_sets_up(
    make_node: NodeFactory,
) -> None:
    """Entries the renewal tick sets up (they were waiting) count as promotion too.

    The tick starts those setups in the background, next to the promotion's
    own loop, which skips entries already being set up: the loop can finish
    first. The endpoint must not turn green before they are set up.
    """
    a = await make_node("node_a")
    await wait_for(lambda: a.cluster.serves_as_leader, timeout=15)
    leases = a.cluster.leases
    assert await a.hass.config_entries.async_unload(a.entry.entry_id)
    real_setup = a.hass.config_entries.async_setup
    proceed = asyncio.Event()
    reached = asyncio.Event()

    async def _slow_setup(entry_id: str, **kwargs: Any) -> bool:
        reached.set()
        await proceed.wait()
        return await real_setup(entry_id, **kwargs)

    view = ClusterLeaderView(a.cluster)
    with patch.object(a.hass.config_entries, "async_setup", _slow_setup):
        leases._standby.add(a.entry.entry_id)
        leases._async_enforce()  # what the next renewal tick does
        await asyncio.wait_for(reached.wait(), 5)
        assert leases.leads
        assert (await view.get(Mock())).status == HTTPStatus.SERVICE_UNAVAILABLE
        proceed.set()
        await wait_for(lambda: a.entry.state is ConfigEntryState.LOADED, timeout=15)
    assert (await view.get(Mock())).status == HTTPStatus.OK


async def test_node_without_nats_is_not_the_leader(make_node: NodeFactory) -> None:
    """A node that cannot reach NATS never answers 200."""
    a = await make_node("node_a", url="nats://127.0.0.1:1")
    assert not a.cluster.started
    assert not a.cluster.serves_as_leader
    view = ClusterLeaderView(a.cluster)
    assert (await view.get(Mock())).status == HTTPStatus.SERVICE_UNAVAILABLE
