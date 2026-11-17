"""Multi-node cluster tests against a real NATS JetStream server."""

import asyncio
from collections.abc import Callable
import json
import time
from typing import Any

from nats.js.errors import KeyNotFoundError
import pytest

from homeassistant.cluster.const import (
    DOMAIN,
    LEADER_KEY,
    STANDBY_WAITING_FOR_COORDINATOR,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import Event, EventOrigin, HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import cluster as cluster_helper
from homeassistant.setup import async_setup_component

from .conftest import (
    DEV_DOMAIN,
    DEV_ENTITY,
    DEV_ENTRY_ID,
    HardExit,
    NodeFactory,
    cluster_config,
    open_entry_payload,
    setup_cluster,
)
from .tcp_proxy import BlackholeProxy


async def wait_for(predicate: Callable[[], bool], timeout: float = 10.0) -> float:
    """Poll until ``predicate`` is true; return the elapsed seconds."""
    start = time.monotonic()
    while not predicate():
        if time.monotonic() - start > timeout:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(0.05)
    return time.monotonic() - start


def record_calls(hass: HomeAssistant) -> list[ServiceCall]:
    """Register ``test.record`` on one node and return its calls."""
    calls: list[ServiceCall] = []

    async def _record(call: ServiceCall) -> None:
        calls.append(call)

    hass.services.async_register("test", "record", _record)
    return calls


AUTOMATION = {
    "automation": {
        "id": "on_button",
        "alias": "On button",
        "triggers": {"trigger": "event", "event_type": "cluster_test_button"},
        "actions": {"action": "test.record"},
    }
}


async def test_entry_runs_on_exactly_one_node(make_node: NodeFactory) -> None:
    """The first node to set up the entry wins; the other stands by."""
    a = await make_node("node_a")
    b = await make_node("node_b")

    assert a.entry.state is ConfigEntryState.LOADED
    assert b.entry.state is ConfigEntryState.NOT_LOADED
    assert b.entry.reason == "Cluster standby: running on node node_a"
    assert a.cluster.leases.is_leader
    assert cluster_helper.async_is_leader(a.hass)
    assert not cluster_helper.async_is_leader(b.hass)


async def test_state_propagates_without_loops(make_node: NodeFactory) -> None:
    """A's switch is mirrored to B, and nothing is ever written back."""
    a = await make_node("node_a")
    b = await make_node("node_b")

    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is not None)
    assert b.hass.states.get(DEV_ENTITY).state == "off"
    assert b.hass.states.get(DEV_ENTITY).attributes["served_by"] == "node_a"

    await a.hass.services.async_call(
        "switch", "turn_on", {"entity_id": DEV_ENTITY}, blocking=True
    )
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY).state == "on")

    kv = a.cluster.coordinator.kv_states
    rev = (await kv.get(DEV_ENTITY)).revision
    await asyncio.sleep(2.5)  # several renew/flush cycles on both nodes
    assert (await kv.get(DEV_ENTITY)).revision == rev, "state echoed back: loop"
    assert DEV_ENTITY not in a.cluster.replicator.mirrored
    assert b.cluster.replicator.mirrored[DEV_ENTITY][0] == "node_a"


async def test_service_call_routed_to_owner(make_node: NodeFactory) -> None:
    """Calling the switch on the standby executes on the owner."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is not None)

    # B has no switch platform loaded for this entry at all.
    await b.hass.services.async_call(
        "switch", "turn_on", {"entity_id": DEV_ENTITY}, blocking=True
    )
    assert a.hass.states.get(DEV_ENTITY).state == "on"
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY).state == "on")

    await b.hass.services.async_call(
        "switch", "turn_off", target={"entity_id": [DEV_ENTITY]}, blocking=False
    )
    await wait_for(lambda: a.hass.states.get(DEV_ENTITY).state == "off")


async def test_failover_on_network_drop(
    make_node: NodeFactory, proxy: BlackholeProxy, hard_exit: HardExit
) -> None:
    """Primary loses its network: it fences itself, then the standby promotes."""
    a = await make_node("node_a", proxy.url)
    b = await make_node("node_b")
    rec = make_node.recorder
    assert a.entry.state is ConfigEntryState.LOADED

    cut_at = time.monotonic()
    proxy.cut()

    await wait_for(lambda: b.entry.state is ConfigEntryState.LOADED, timeout=20)
    failover = time.monotonic() - cut_at
    await wait_for(lambda: a.entry.state is ConfigEntryState.NOT_LOADED, timeout=5)

    a_unload = rec.times("node_a", "unload")[-1]
    b_setup = rec.times("node_b", "setup")[-1]
    assert a_unload < b_setup, "split brain: B started before A stopped"
    leases = b.cluster.leases
    # A fences before the server can expire its key, and B waits the unload
    # grace after winning the expired key before it starts the entry.
    assert a_unload - cut_at < a.cluster.leases.fence_after + 0.3 < leases.ttl
    assert (
        b_setup - a_unload
        >= leases.ttl - leases.fence_after + leases.unload_grace - 0.3
    )
    assert failover < leases.ttl + leases.unload_grace + 2 * leases.renew_interval + 2.0
    await wait_for(lambda: b.cluster.leases.is_leader, timeout=10)
    assert not a.cluster.leases.is_leader
    assert not cluster_helper.async_is_leader(a.hass)

    assert b.hass.states.get(DEV_ENTITY).attributes["served_by"] == "node_b"
    assert DEV_ENTITY not in b.cluster.replicator.mirrored

    # When A's network comes back it stays standby and mirrors B.
    proxy.restore()
    await wait_for(
        lambda: (
            (s := a.hass.states.get(DEV_ENTITY)) is not None
            and s.attributes.get("served_by") == "node_b"
        ),
        timeout=15,
    )
    assert a.entry.state is ConfigEntryState.NOT_LOADED
    # A normal failover never needs the watchdog.
    assert not hard_exit.times


async def test_lease_expiry_without_release(make_node: NodeFactory) -> None:
    """A node that dies without releasing is replaced after TTL plus grace."""
    await make_node("node_a")
    b = await make_node("node_b")

    # Simulate a crash: stop renewing, release nothing, leave entry "running".
    await make_node.nodes[0].cluster.leases.async_stop(release=False)
    elapsed = await wait_for(lambda: b.entry.state is ConfigEntryState.LOADED, 15)
    leases = b.cluster.leases
    assert leases.ttl - 1.0 <= elapsed <= leases.ttl + leases.unload_grace + 3.0


async def test_graceful_handover_is_fast(
    make_node: NodeFactory, hard_exit: HardExit
) -> None:
    """Releasing on shutdown hands over well before the TTL, without grace."""
    a = await make_node("node_a", lease_ttl=10.0, renew_interval=1.0)
    b = await make_node("node_b", lease_ttl=10.0, renew_interval=1.0)

    await a.hass.config_entries.async_unload(DEV_ENTRY_ID)
    await a.cluster.leases.async_stop(release=True)
    elapsed = await wait_for(lambda: b.entry.state is ConfigEntryState.LOADED, 5)
    assert elapsed < 3.0
    assert (await b.cluster.coordinator.kv_leases.get(LEADER_KEY)).value
    assert not hard_exit.times


async def test_config_entry_data_follows_failover(make_node: NodeFactory) -> None:
    """A token rotated on the owner is what the standby starts with."""
    a = await make_node("node_a")
    b = await make_node("node_b")

    a.hass.config_entries.async_update_entry(a.entry, data={"token": "t2"})
    await a.hass.async_block_till_done()
    kv = a.cluster.coordinator.kv_storage

    async def published() -> bool:
        try:
            entry = await kv.get(f"entry.{DEV_ENTRY_ID}")
        except Exception:  # noqa: BLE001
            return False
        return (
            b'"t2"'
            in json.dumps(
                open_entry_payload(f"entry.{DEV_ENTRY_ID}", entry.value)
            ).encode()
        )

    deadline = time.monotonic() + 10
    while not await published():
        assert time.monotonic() < deadline, "entry data was never mirrored"
        await asyncio.sleep(0.2)

    await a.cluster.leases.async_stop(release=True)
    await wait_for(lambda: b.entry.state is ConfigEntryState.LOADED, 10)
    assert make_node.recorder.events[-1][3]["token"] == "t2"
    assert b.entry.data["token"] == "t2"


async def test_entries_stay_on_standby_until_nats_is_reachable(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """Fail closed at boot: without NATS nothing clustered starts."""
    proxy.cut()
    a = await make_node("node_a", proxy.url)

    assert a.entry.state is ConfigEntryState.NOT_LOADED
    assert a.entry.reason == STANDBY_WAITING_FOR_COORDINATOR
    assert not cluster_helper.async_is_leader(a.hass)
    assert not make_node.recorder.times("node_a", "setup")

    proxy.restore()
    await wait_for(lambda: a.entry.state is ConfigEntryState.LOADED, timeout=30)
    await wait_for(lambda: cluster_helper.async_is_leader(a.hass), timeout=10)


async def test_entry_started_before_cluster_is_reconciled(
    make_node: NodeFactory, clean_nats: str
) -> None:
    """An entry that loaded before the cluster is unloaded if leased elsewhere."""
    a = await make_node("node_a")
    b = await make_node("node_b", start=False)

    assert await async_setup_component(b.hass, DEV_DOMAIN, {})
    await b.hass.async_block_till_done()
    assert b.entry.state is ConfigEntryState.LOADED

    assert await setup_cluster(b.hass, cluster_config("node_b", clean_nats))
    await wait_for(lambda: b.entry.state is ConfigEntryState.NOT_LOADED, 5)
    assert a.entry.state is ConfigEntryState.LOADED


async def test_single_node_cluster_keeps_working(make_node: NodeFactory) -> None:
    """One configured node behaves like plain Home Assistant."""
    a = await make_node("node_a")
    await a.hass.services.async_call(
        "switch", "toggle", {"entity_id": DEV_ENTITY}, blocking=True
    )
    assert a.hass.states.get(DEV_ENTITY).state == "on"
    assert a.cluster.leases.holds(DEV_ENTRY_ID)


@pytest.mark.parametrize(
    "bad",
    [
        pytest.param({"node_id": "node A"}, id="node_id_not_subject_safe"),
        pytest.param({"node_id": "a", "lease_ttl": 1.5}, id="renewal_too_slow"),
    ],
)
async def test_invalid_config_rejected(
    hass: HomeAssistant, bad: dict[str, Any]
) -> None:
    """Node ids must be subject-safe; renewals must fit twice into the TTL."""
    assert not await setup_cluster(hass, {DOMAIN: bad})


async def test_yaml_entities_are_not_squatted(make_node: NodeFactory) -> None:
    """The leader's YAML entity must not take the id of the follower's own copy.

    Every node runs its YAML entities, so the leader does not publish them.
    """
    conf = {"input_boolean": {"probe": {"name": "Probe"}}}
    a = await make_node("node_a")
    assert await async_setup_component(a.hass, "input_boolean", conf)
    await a.hass.async_block_till_done()
    await asyncio.sleep(1)
    with pytest.raises(KeyNotFoundError):
        await a.cluster.coordinator.kv_states.get("input_boolean.probe")

    b = await make_node("node_b")
    await asyncio.sleep(0.5)
    assert await async_setup_component(b.hass, "input_boolean", conf)
    await b.hass.async_block_till_done()
    assert b.hass.states.get("input_boolean.probe") is not None
    assert b.hass.states.get("input_boolean.probe_2") is None
    assert "input_boolean.probe" not in b.cluster.replicator.mirrored


async def test_yaml_entity_calls_go_to_the_leader(make_node: NodeFactory) -> None:
    """A call for an entity without a config entry runs on the leader."""
    conf = {"input_boolean": {"probe": {"name": "Probe"}}}
    a = await make_node("node_a")
    b = await make_node("node_b")
    for node in (a, b):
        assert await async_setup_component(node.hass, "input_boolean", conf)
    await b.hass.async_block_till_done()

    await b.hass.services.async_call(
        "input_boolean", "turn_on", {"entity_id": "input_boolean.probe"}, blocking=True
    )
    assert a.hass.states.get("input_boolean.probe").state == "on"


async def test_events_are_shared_once(make_node: NodeFactory) -> None:
    """An event fired on one node fires once on the other, as remote."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    seen: dict[str, list[Event]] = {"node_a": [], "node_b": []}
    for node in (a, b):
        node.hass.bus.async_listen("cluster_test_button", seen[node.name].append)

    b.hass.bus.async_fire("cluster_test_button", {"button": 1})
    await wait_for(lambda: len(seen["node_a"]) == 1)
    await asyncio.sleep(0.5)

    assert [e.origin for e in seen["node_a"]] == [EventOrigin.remote]
    assert seen["node_a"][0].data == {"button": 1}
    assert [e.origin for e in seen["node_b"]] == [EventOrigin.local]


async def test_automation_runs_once_on_the_leader(make_node: NodeFactory) -> None:
    """A trigger on a follower runs the automation once, on the leader."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    calls = {node.name: record_calls(node.hass) for node in (a, b)}
    for node in (a, b):
        assert await async_setup_component(node.hass, "automation", AUTOMATION)
    await b.hass.async_block_till_done()

    b.hass.bus.async_fire("cluster_test_button")
    await wait_for(lambda: len(calls["node_a"]) == 1)
    await asyncio.sleep(0.5)

    assert len(calls["node_a"]) == 1
    assert calls["node_b"] == []
    assert b.hass.states.get("automation.on_button").state == "on"


async def test_automations_move_with_leadership(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """The new leader runs the automations; the old one stops."""
    a = await make_node("node_a", proxy.url)
    b = await make_node("node_b")
    calls = {node.name: record_calls(node.hass) for node in (a, b)}
    for node in (a, b):
        assert await async_setup_component(node.hass, "automation", AUTOMATION)
    await b.hass.async_block_till_done()

    proxy.cut()
    await wait_for(lambda: cluster_helper.async_is_leader(b.hass), timeout=20)
    assert not cluster_helper.async_is_leader(a.hass)
    await b.hass.async_block_till_done()

    for node in (a, b):
        node.hass.bus.async_fire("cluster_test_button")
    await wait_for(lambda: len(calls["node_b"]) == 1)
    await asyncio.sleep(0.5)
    assert len(calls["node_b"]) == 1
    assert calls["node_a"] == []


async def test_script_runs_on_the_leader(make_node: NodeFactory) -> None:
    """Starting a script on a follower runs it on the leader only."""
    conf = {"script": {"record": {"sequence": [{"action": "test.record"}]}}}
    a = await make_node("node_a")
    b = await make_node("node_b")
    calls = {node.name: record_calls(node.hass) for node in (a, b)}
    for node in (a, b):
        assert await async_setup_component(node.hass, "script", conf)
    await b.hass.async_block_till_done()

    await b.hass.services.async_call("script", "record", blocking=True)
    await b.hass.services.async_call(
        "script", "turn_on", {"entity_id": "script.record"}, blocking=True
    )
    await a.hass.async_block_till_done()

    assert len(calls["node_a"]) == 2
    assert calls["node_b"] == []


async def test_follower_without_leader_refuses_scripts(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """With no reachable leader, a follower does not run a script itself."""
    conf = {"script": {"record": {"sequence": [{"action": "test.record"}]}}}
    a = await make_node("node_a")
    b = await make_node("node_b", proxy.url)
    calls = record_calls(b.hass)
    assert await async_setup_component(b.hass, "script", conf)
    await b.hass.async_block_till_done()
    assert a.cluster.leases.is_leader

    proxy.cut()
    await wait_for(lambda: b.cluster.leases.leader() is None, timeout=10)
    with pytest.raises(Exception, match="no leader|did not answer|NATS is down"):
        await b.hass.services.async_call("script", "record", blocking=True)
    assert calls == []


async def test_former_leader_refuses_forwarded_scripts(make_node: NodeFactory) -> None:
    """A node that is not the leader does not run a script sent to it."""
    conf = {"script": {"record": {"sequence": [{"action": "test.record"}]}}}
    a = await make_node("node_a")
    b = await make_node("node_b")
    calls = record_calls(b.hass)
    assert await async_setup_component(b.hass, "script", conf)
    await b.hass.async_block_till_done()

    # A believes B is the leader (a stale view).
    a.cluster.leases._owners[LEADER_KEY] = ("node_b", time.monotonic())
    a.cluster.leases._held.pop(LEADER_KEY).fence.cancel()
    with pytest.raises(HomeAssistantError, match="not the cluster leader"):
        await a.hass.services.async_call("script", "record", blocking=True)
    assert calls == []
