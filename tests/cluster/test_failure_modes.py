"""Failure modes of the cluster, against a real NATS JetStream server."""

import asyncio
from collections.abc import Awaitable, Callable
import json
import logging
import time
from unittest.mock import patch

import nats
from nats.js.api import KeyValueConfig, StorageType
from nats.js.errors import KeyNotFoundError
import pytest
from pytest_homeassistant.common import (
    MockEntity,
    MockModule,
    MockPlatform,
    mock_integration,
    mock_platform,
)

from homeassistant.auth.const import GROUP_ID_ADMIN, GROUP_ID_READ_ONLY, GROUP_ID_USER
from homeassistant.cluster.const import LEADER_KEY
from homeassistant.cluster.lease_manager import LeaseManager
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import Context, Event, EventOrigin, HomeAssistant, ServiceCall
from homeassistant.exceptions import (
    ConfigEntryNotReady,
    HomeAssistantError,
    Unauthorized,
)
from homeassistant.helpers import cluster as cluster_helper, entity_registry as er
from homeassistant.helpers.entity_platform import (
    AddConfigEntryEntitiesCallback,
    AddEntitiesCallback,
)
from homeassistant.helpers.service import async_register_admin_service
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.setup import async_setup_component
from homeassistant.util.json import json_loads_object

from .conftest import (
    DEV_DOMAIN,
    DEV_ENTITY,
    DEV_ENTRY_ID,
    DevRecorder,
    DevSwitch,
    NodeFactory,
    cluster_config,
    open_entry_payload,
    setup_cluster,
    start_node,
)
from .tcp_proxy import BlackholeProxy
from .test_init import AUTOMATION, record_calls, wait_for

LOADED = ConfigEntryState.LOADED
NOT_LOADED = ConfigEntryState.NOT_LOADED

type Hook = Callable[[HomeAssistant, ConfigEntry], Awaitable[None]]


def install_custom(
    hass: HomeAssistant,
    node: str,
    rec: DevRecorder,
    *,
    setup_delay: float = 0.0,
    supports_unload: bool = True,
    unload_raises: bool = False,
    remove_raises: bool = False,
    not_ready: bool = False,
    on_setup_begin: Callable[[], None] | None = None,
    extra_setup: Hook | None = None,
) -> None:
    """Replace the fake integration on one node with a configurable one.

    With ``not_ready``, every setup fails after ``setup_delay`` seconds.
    """

    async def setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "setup_begin", dict(entry.data)))
        if on_setup_begin is not None:
            on_setup_begin()
        if setup_delay:
            await asyncio.sleep(setup_delay)
        if not_ready:
            rec.events.append((time.monotonic(), node, "setup_failed", {}))
            raise ConfigEntryNotReady("the device is not reachable from this node")
        if extra_setup is not None:
            await extra_setup(hass, entry)
        await hass.config_entries.async_forward_entry_setups(entry, ["switch"])
        rec.events.append((time.monotonic(), node, "setup", dict(entry.data)))

    async def unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "unload_begin", dict(entry.data)))
        if unload_raises:
            raise RuntimeError("the device did not acknowledge the disconnect")
        await hass.config_entries.async_unload_platforms(entry, ["switch"])
        rec.events.append((time.monotonic(), node, "unload", dict(entry.data)))

    async def remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "cleanup", dict(entry.data)))
        if remove_raises:
            raise RuntimeError("the cloud resource could not be deleted")

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
            async_unload_entry=unload_entry if supports_unload else None,
            async_remove_entry=remove_entry,
        ),
    )
    mock_platform(
        hass, f"{DEV_DOMAIN}.switch", MockPlatform(async_setup_entry=switch_setup_entry)
    )
    mock_platform(hass, f"{DEV_DOMAIN}.config_flow", None)


def served_by(hass: HomeAssistant) -> str | None:
    """Which node's entity object stands behind the dev switch state here."""
    state = hass.states.get(DEV_ENTITY)
    return None if state is None else state.attributes.get("served_by")


async def test_fence_during_setup_unloads_once_set_up(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """A lease lost while the entry sets up is enforced after the setup."""
    rec = make_node.recorder
    a = await make_node("node_a", proxy.url, start=False)
    in_setup = asyncio.Event()
    install_custom(a.hass, "node_a", rec, setup_delay=4.0, on_setup_begin=in_setup.set)
    start_a = asyncio.create_task(start_node(a, proxy.url))
    await asyncio.wait_for(in_setup.wait(), 15)
    proxy.cut()
    b = await make_node("node_b")

    await wait_for(lambda: b.entry.state is LOADED, timeout=25)
    await start_a
    await wait_for(lambda: a.entry.state is NOT_LOADED, timeout=10)
    assert rec.times("node_a", "unload")
    assert b.entry.state is LOADED

    proxy.restore()
    await asyncio.sleep(3)
    assert (a.entry.state, b.entry.state) == (NOT_LOADED, LOADED)


@pytest.mark.parametrize(
    ("supports_unload", "unload_raises"),
    [
        pytest.param(False, False, id="unload_not_supported"),
        pytest.param(True, True, id="unload_raises"),
    ],
)
async def test_failed_unload_after_fence_restarts(
    make_node: NodeFactory,
    proxy: BlackholeProxy,
    supports_unload: bool,
    unload_raises: bool,
) -> None:
    """An entry that cannot be unloaded after a fence restarts the node."""
    rec = make_node.recorder
    a = await make_node("node_a", proxy.url, start=False)
    install_custom(
        a.hass,
        "node_a",
        rec,
        supports_unload=supports_unload,
        unload_raises=unload_raises,
    )
    with patch.object(LeaseManager, "async_restart") as restart:
        await start_node(a, proxy.url)
        b = await make_node("node_b")
        assert a.entry.state is LOADED

        proxy.cut()
        await wait_for(lambda: restart.called, timeout=15)
        assert a.entry.state is ConfigEntryState.FAILED_UNLOAD
        # The restart happens before the other node can start the entry.
        assert b.entry.state is NOT_LOADED


async def test_unload_grace_above_ttl_elects_one_leader(
    make_node: NodeFactory,
) -> None:
    """The first election renews its lease while it waits the grace."""
    a, b = await asyncio.gather(
        make_node("node_a", unload_grace=4.0), make_node("node_b", unload_grace=4.0)
    )
    calls = {node.name: record_calls(node.hass) for node in (a, b)}
    for node in (a, b):
        assert await async_setup_component(node.hass, "automation", AUTOMATION)
    await asyncio.sleep(6)

    flags = [cluster_helper.async_is_leader(n.hass) for n in (a, b)]
    assert flags.count(True) == 1
    assert [n.cluster.leases.leads for n in (a, b)] == flags
    a.hass.bus.async_fire("cluster_test_button")
    await asyncio.sleep(1.5)
    assert sum(len(c) for c in calls.values()) == 1


async def test_duplicate_node_id_stands_by(make_node: NodeFactory) -> None:
    """A second instance with the same node_id stays on standby."""
    a = await make_node("node_a")
    b = await make_node("node_b", start=False)
    assert await setup_cluster(b.hass, cluster_config("node_a", make_node.url))
    assert await async_setup_component(b.hass, DEV_DOMAIN, {})
    await b.hass.async_block_till_done()
    await asyncio.sleep(6)

    assert a.entry.state is LOADED
    assert b.entry.state is NOT_LOADED
    assert cluster_helper.async_is_leader(a.hass)
    assert not cluster_helper.async_is_leader(b.hass)
    assert not b.cluster.leases.held


async def test_lease_ttl_mismatch_uses_the_bucket_ttl(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """A node fences by the TTL the lease bucket really has."""
    nc = await nats.connect(make_node.url)
    await nc.jetstream().create_key_value(
        KeyValueConfig(
            bucket="hass_leases",
            history=1,
            ttl=3.0,
            storage=StorageType.MEMORY,
            replicas=1,
        )
    )
    await nc.close()
    rec = make_node.recorder
    a = await make_node("node_a", proxy.url, lease_ttl=10.0, renew_interval=1.0)
    b = await make_node("node_b")
    assert a.entry.state is LOADED
    assert a.cluster.leases.ttl == pytest.approx(3.0)

    proxy.cut()
    await wait_for(lambda: b.entry.state is LOADED, timeout=25)
    await wait_for(lambda: a.entry.state is NOT_LOADED, timeout=25)
    assert rec.times("node_a", "unload")[-1] < rec.times("node_b", "setup")[-1]


async def test_shutdown_publishes_then_hands_over(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """A stopping owner publishes its changes before it releases.

    The token rotated right before the stop (still in the delayed save) is
    what the other node starts with, the other node takes over on the
    release (no grace), and the stopping node stops leading at once.
    """
    caplog.set_level(logging.INFO, logger="homeassistant.cluster")
    a = await make_node("node_a", start=False)
    b = await make_node("node_b")
    await start_node(a, make_node.url)
    assert b.entry.state is LOADED
    assert cluster_helper.async_is_leader(b.hass)

    leader_flags: list[bool] = []
    cluster_helper.async_track_leadership(b.hass, leader_flags.append)
    b.hass.config_entries.async_update_entry(b.entry, data={"token": "t2"})
    stop = asyncio.create_task(b.hass.async_stop())
    await wait_for(lambda: leader_flags == [False], timeout=5)
    await stop
    await wait_for(lambda: a.entry.state is LOADED, timeout=5)

    assert a.entry.data["token"] == "t2"
    assert f"Node node_a won {LEADER_KEY} after" not in caplog.text
    await wait_for(lambda: cluster_helper.async_is_leader(a.hass), timeout=5)
    # Unloaded once, before the release (the close does not unload again).
    assert len(make_node.recorder.times("node_b", "unload")) == 1


async def test_buckets_lost_by_nats_are_recreated(make_node: NodeFactory) -> None:
    """The cluster recovers when NATS loses its memory-backed buckets."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    assert a.entry.state is LOADED

    nc = await nats.connect(make_node.url)
    js = nc.jetstream()
    await js.delete_stream("KV_hass_leases")
    await js.delete_stream("KV_hass_states")

    async def recreated() -> bool:
        try:
            await js.stream_info("KV_hass_leases")
            await js.stream_info("KV_hass_states")
        except Exception:  # noqa: BLE001
            return False
        return True

    deadline = time.monotonic() + 15
    while not await recreated():
        assert time.monotonic() < deadline, "the buckets were never recreated"
        await asyncio.sleep(0.2)
    await nc.close()

    # The old holder fences and the leases are elected again, with the grace.
    leases = a.cluster.leases
    await asyncio.sleep(leases.ttl + leases.unload_grace + 2)
    await wait_for(
        lambda: (a.entry.state, b.entry.state).count(LOADED) == 1, timeout=20
    )
    await wait_for(
        lambda: (
            [cluster_helper.async_is_leader(n.hass) for n in (a, b)].count(True) == 1
        ),
        timeout=15,
    )
    await asyncio.sleep(2)
    assert (a.entry.state, b.entry.state).count(LOADED) == 1
    owner = a if a.entry.state is LOADED else b
    other = b if owner is a else a
    await wait_for(lambda: DEV_ENTITY in other.cluster.replicator.mirrored, timeout=10)
    # The leader endpoint follows: 200 on the leader only.
    await wait_for(lambda: owner.cluster.serves_as_leader, timeout=10)
    assert not other.cluster.serves_as_leader


async def test_owner_reload_keeps_the_change(make_node: NodeFactory) -> None:
    """An update followed by a reload on the owner is kept."""
    a = await make_node("node_a")
    hass = a.hass
    kv = a.cluster.coordinator.kv_storage

    hass.config_entries.async_update_entry(a.entry, options={"mode": "v1"})

    async def published(value: bytes) -> bool:
        try:
            raw = (await kv.get(f"entry.{DEV_ENTRY_ID}")).value
            return (
                value
                in json.dumps(open_entry_payload(f"entry.{DEV_ENTRY_ID}", raw)).encode()
            )
        except KeyNotFoundError:
            return False

    deadline = time.monotonic() + 5
    while not await published(b'"v1"'):
        assert time.monotonic() < deadline, "the change was never published"
        await asyncio.sleep(0.1)

    hass.config_entries.async_update_entry(a.entry, options={"mode": "v2"})
    await hass.config_entries.async_reload(DEV_ENTRY_ID)
    await hass.async_block_till_done()
    assert dict(a.entry.options) == {"mode": "v2"}
    # Published right away, not after the delayed save.
    deadline = time.monotonic() + 1
    while not await published(b'"v2"'):
        assert time.monotonic() < deadline, "the change was not published at once"
        await asyncio.sleep(0.05)


async def test_service_of_another_domain_reaches_its_owner(
    make_node: NodeFactory,
) -> None:
    """Notify.<name> registered by an entry is found on its owner."""
    received: list[ServiceCall] = []

    async def register_notify(hass: HomeAssistant, entry: ConfigEntry) -> None:
        async def _notify(call: ServiceCall) -> None:
            received.append(call)

        hass.services.async_register("notify", "dev_phone", _notify)

    rec = make_node.recorder
    a = await make_node("node_a", start=False)
    install_custom(a.hass, "node_a", rec, extra_setup=register_notify)
    await start_node(a, make_node.url)
    b = await make_node("node_b", start=False)
    install_custom(b.hass, "node_b", rec, extra_setup=register_notify)
    await start_node(b, make_node.url)
    assert (a.entry.state, b.entry.state) == (LOADED, NOT_LOADED)

    await b.hass.services.async_call(
        "notify", "dev_phone", {"message": "door open"}, blocking=True
    )
    assert len(received) == 1
    with pytest.raises(HomeAssistantError):
        await b.hass.services.async_call("notify", "nobody", {}, blocking=True)


async def test_failing_setup_is_handed_to_another_node(make_node: NodeFactory) -> None:
    """A node that cannot set an entry up hands it to another node."""
    rec = make_node.recorder
    b = await make_node("node_b", start=False)
    install_custom(b.hass, "node_b", rec, not_ready=True)
    await start_node(b, make_node.url)
    assert b.entry.state is ConfigEntryState.SETUP_RETRY
    a = await make_node("node_a")

    await wait_for(lambda: a.entry.state is LOADED, timeout=20)
    assert b.entry.state is NOT_LOADED
    assert not b.cluster.leases.holds(DEV_ENTRY_ID)
    assert b.cluster.leases._failing_since is None


async def test_broadcast_reaches_a_pure_standby(make_node: NodeFactory) -> None:
    """Automation.turn_off on the leader also reaches a node owning nothing."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    calls = {node.name: record_calls(node.hass) for node in (a, b)}
    for node in (a, b):
        assert await async_setup_component(node.hass, "automation", AUTOMATION)
    await b.hass.async_block_till_done()
    await wait_for(lambda: "node_b" in a.cluster.leases.members, timeout=5)

    await a.hass.services.async_call(
        "automation", "turn_off", {"entity_id": "automation.on_button"}, blocking=True
    )
    await wait_for(
        lambda: b.hass.states.get("automation.on_button").state == "off", timeout=5
    )

    await a.hass.config_entries.async_unload(DEV_ENTRY_ID)
    await a.cluster.leases.async_stop(release=True)
    await wait_for(lambda: cluster_helper.async_is_leader(b.hass), timeout=10)
    await b.hass.async_block_till_done()
    b.hass.bus.async_fire("cluster_test_button")
    await asyncio.sleep(1)
    assert calls["node_b"] == []


async def test_grace_decided_by_the_server(
    make_node: NodeFactory, proxy: BlackholeProxy, caplog: pytest.LogCaptureFixture
) -> None:
    """An expired key gets the grace even after the standby was blind."""
    caplog.set_level(logging.INFO, logger="homeassistant.cluster")
    a = await make_node("node_a")
    b = await make_node("node_b", proxy.url)
    assert a.entry.state is LOADED
    ttl = b.cluster.leases.ttl

    proxy.cut()
    await asyncio.sleep(3 * ttl + 1)
    await a.cluster.leases.async_stop(release=False)  # A stops renewing
    await asyncio.sleep(ttl + 0.5)  # A's keys expire on the server
    proxy.restore()
    await wait_for(lambda: b.entry.state is LOADED, timeout=30)

    assert f"Node node_b won {LEADER_KEY} after" in caplog.text


async def test_non_admin_cannot_run_a_remote_admin_service(
    make_node: NodeFactory,
) -> None:
    """An admin service on another node is refused for a non-admin."""
    admin_calls: list[ServiceCall] = []

    async def register_admin(hass: HomeAssistant, entry: ConfigEntry) -> None:
        async def _admin(call: ServiceCall) -> None:
            admin_calls.append(call)

        async_register_admin_service(hass, DEV_DOMAIN, "factory_reset", _admin)

    rec = make_node.recorder
    a = await make_node("node_a", start=False)
    install_custom(a.hass, "node_a", rec, extra_setup=register_admin)
    await start_node(a, make_node.url)
    b = await make_node("node_b")
    await b.hass.auth.async_create_user("owner")  # the first user is the owner
    kid = await b.hass.auth.async_create_user("kid", group_ids=[GROUP_ID_USER])
    admin = await b.hass.auth.async_create_user("parent", group_ids=[GROUP_ID_ADMIN])

    with pytest.raises(HomeAssistantError):
        await b.hass.services.async_call(
            DEV_DOMAIN, "factory_reset", blocking=True, context=Context(user_id=kid.id)
        )
    assert admin_calls == []

    await b.hass.services.async_call(
        DEV_DOMAIN, "factory_reset", blocking=True, context=Context(user_id=admin.id)
    )
    assert len(admin_calls) == 1

    with pytest.raises(HomeAssistantError):  # unknown on the calling node too
        await b.hass.services.async_call(
            DEV_DOMAIN, "factory_reset", blocking=True, context=Context(user_id="nope")
        )
    assert len(admin_calls) == 1


async def test_entity_id_all_respects_permissions(make_node: NodeFactory) -> None:
    """Entity_id all from a read-only user does not control remote entities."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    assert await async_setup_component(b.hass, "switch", {})
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is not None)
    await b.hass.auth.async_create_user("owner")
    guest = await b.hass.auth.async_create_user("guest", group_ids=[GROUP_ID_READ_ONLY])

    await b.hass.services.async_call(
        "switch",
        "turn_on",
        {"entity_id": "all"},
        blocking=True,
        context=Context(user_id=guest.id),
    )
    await asyncio.sleep(0.5)
    assert a.hass.states.get(DEV_ENTITY).state == "off"

    await b.hass.services.async_call(
        "switch", "turn_on", {"entity_id": "all"}, blocking=True
    )
    await wait_for(lambda: a.hass.states.get(DEV_ENTITY).state == "on")


async def test_per_node_entries_are_not_mirrored(make_node: NodeFactory) -> None:
    """Entities of an entry each node runs itself are not published."""
    a = await make_node("node_a", exclude_domains=[DEV_DOMAIN])
    b = await make_node("node_b", exclude_domains=[DEV_DOMAIN])
    assert (a.entry.state, b.entry.state) == (LOADED, LOADED)
    await asyncio.sleep(1)

    assert served_by(a.hass) == "node_a"
    assert served_by(b.hass) == "node_b"
    assert DEV_ENTITY not in b.cluster.replicator.mirrored
    with pytest.raises(KeyNotFoundError):
        await a.cluster.coordinator.kv_states.get(DEV_ENTITY)


async def test_scripts_refused_without_leadership(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """A node that is not leader (NATS unreachable at boot) runs no script."""
    proxy.cut()
    a = await make_node("node_a", proxy.url)
    calls = record_calls(a.hass)
    conf = {"script": {"run_me": {"sequence": [{"action": "test.record"}]}}}
    assert await async_setup_component(a.hass, "script", conf)
    assert await async_setup_component(a.hass, "automation", AUTOMATION)
    await a.hass.async_block_till_done()
    assert not cluster_helper.async_is_leader(a.hass)

    with pytest.raises(HomeAssistantError, match="not the cluster leader"):
        await a.hass.services.async_call("script", "run_me", blocking=True)
    with pytest.raises(HomeAssistantError, match="not the cluster leader"):
        await a.hass.services.async_call(
            "automation",
            "trigger",
            {"entity_id": "automation.on_button"},
            blocking=True,
        )
    assert calls == []


async def test_homeassistant_services_reach_the_owner(make_node: NodeFactory) -> None:
    """Homeassistant.turn_on on a standby without the domain loaded."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    for node in (a, b):
        assert await async_setup_component(node.hass, "homeassistant", {})
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is not None)
    assert not b.hass.services.has_service("switch", "turn_on")

    await b.hass.services.async_call(
        "homeassistant", "turn_on", {"entity_id": DEV_ENTITY}, blocking=True
    )
    assert a.hass.states.get(DEV_ENTITY).state == "on"


async def test_event_sharing_rules(make_node: NodeFactory) -> None:
    """Reload events stay local; remote-origin events are shared once."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    seen: list[Event] = []
    for event_type in ("event_template_reloaded", "webhook_button"):
        a.hass.bus.async_listen(event_type, seen.append)

    b.hass.bus.async_fire("event_template_reloaded")
    b.hass.bus.async_fire("webhook_button", {"n": 1}, EventOrigin.remote)
    await wait_for(lambda: len(seen) >= 1)
    await asyncio.sleep(0.5)

    assert [(e.event_type, e.origin) for e in seen] == [
        ("webhook_button", EventOrigin.remote)
    ]


async def test_stopped_lease_manager_gives_up_leadership(
    make_node: NodeFactory,
) -> None:
    """Stopping the leases (e.g. after a failed start) clears leadership."""
    a = await make_node("node_a")
    assert cluster_helper.async_is_leader(a.hass)
    await a.cluster.leases.async_stop(release=False)
    assert not cluster_helper.async_is_leader(a.hass)


@pytest.mark.parametrize("remove", [False, True], ids=["rename", "remove"])
async def test_renamed_or_removed_entity_is_deleted_everywhere(
    make_node: NodeFactory, entity_registry: er.EntityRegistry, remove: bool
) -> None:
    """The old entity id of a renamed or removed entity leaves the standby."""
    a = await make_node("node_a")  # the first node runs on the hass fixture
    b = await make_node("node_b")
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is not None)

    registry = entity_registry
    if remove:
        registry.async_remove(DEV_ENTITY)
    else:
        registry.async_update_entity(DEV_ENTITY, new_entity_id="switch.renamed")
        await wait_for(lambda: b.hass.states.get("switch.renamed") is not None)
    await a.hass.async_block_till_done()

    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is None, timeout=5)
    with pytest.raises(KeyNotFoundError):
        await a.cluster.coordinator.kv_states.get(DEV_ENTITY)


SWITCH_SCRIPT = {
    "script": {
        "switch_on": {
            "sequence": [
                {"action": "switch.turn_on", "target": {"entity_id": DEV_ENTITY}}
            ]
        }
    }
}


async def test_script_from_another_node_runs_with_the_callers_permissions(
    make_node: NodeFactory,
) -> None:
    """A read-only user's script on the leader cannot switch things.

    The leader does not know the user; Core on a single instance refuses the
    switch.turn_on inside the script, and so must the cluster.
    """
    a = await make_node("node_a")
    b = await make_node("node_b")
    for node in (a, b):
        assert await async_setup_component(node.hass, "script", SWITCH_SCRIPT)
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is not None)
    await b.hass.auth.async_create_user("owner")
    guest = await b.hass.auth.async_create_user("guest", group_ids=[GROUP_ID_READ_ONLY])
    member = await b.hass.auth.async_create_user("member", group_ids=[GROUP_ID_USER])

    with pytest.raises(Unauthorized):
        await b.hass.services.async_call(
            "script", "switch_on", blocking=True, context=Context(user_id=guest.id)
        )
    await asyncio.sleep(0.5)
    assert a.hass.states.get(DEV_ENTITY).state == "off"

    await b.hass.services.async_call(
        "script", "switch_on", blocking=True, context=Context(user_id=member.id)
    )
    await wait_for(lambda: a.hass.states.get(DEV_ENTITY).state == "on")


async def test_yaml_platform_entity_of_an_entry_domain_is_mirrored_not_attributed(
    make_node: NodeFactory,
) -> None:
    """A YAML entity of the leader is not attributed to an entry.

    It is not the entry's, but the platform runs on the leader only, so the
    standby mirrors it.
    """

    async def yaml_sensor_setup(
        hass: HomeAssistant,
        config: ConfigType,
        async_add_entities: AddEntitiesCallback,
        discovery_info: DiscoveryInfoType | None = None,
    ) -> None:
        async_add_entities([MockEntity(name="yaml local sensor", should_poll=False)])

    a = await make_node("node_a")
    b = await make_node("node_b")
    mock_platform(
        a.hass,
        f"{DEV_DOMAIN}.sensor",
        MockPlatform(async_setup_platform=yaml_sensor_setup),
    )
    assert await async_setup_component(
        a.hass, "sensor", {"sensor": [{"platform": DEV_DOMAIN}]}
    )
    await a.hass.async_block_till_done()
    assert a.hass.states.get("sensor.yaml_local_sensor") is not None
    await asyncio.sleep(1)

    assert a.cluster.replicator.entry_of("sensor.yaml_local_sensor") is None
    await wait_for(
        lambda: "sensor.yaml_local_sensor" in b.cluster.replicator.mirrored, timeout=10
    )


async def test_owner_keeps_its_entries_when_the_lease_bucket_is_lost(
    make_node: NodeFactory,
) -> None:
    """A briefly lost lease bucket does not unload the healthy owner."""
    rec = make_node.recorder
    a = await make_node("node_a")
    b = await make_node("node_b")
    assert a.entry.state is LOADED

    nc = await nats.connect(make_node.url)
    await nc.jetstream().delete_stream("KV_hass_leases")
    await nc.close()
    await asyncio.sleep(a.cluster.leases.ttl + a.cluster.leases.unload_grace + 2)

    assert (a.entry.state, b.entry.state) == (LOADED, NOT_LOADED)
    assert not rec.times("node_a", "unload")
    assert a.cluster.leases.holds(DEV_ENTRY_ID)
    assert cluster_helper.async_is_leader(a.hass)
    assert not cluster_helper.async_is_leader(b.hass)


async def test_entry_record_published_when_the_lease_is_won(
    make_node: NodeFactory,
) -> None:
    """The record is published at setup, not only on the next change."""
    a = await make_node("node_a")
    kv = a.cluster.coordinator.kv_storage

    async def published() -> bool:
        try:
            await kv.get(f"entry.{DEV_ENTRY_ID}")
        except KeyNotFoundError:
            return False
        return True

    deadline = time.monotonic() + 5
    while not await published():
        assert time.monotonic() < deadline, "the record was not published"
        await asyncio.sleep(0.1)


async def test_standing_by_duplicate_leaves_the_member_key_alone(
    make_node: NodeFactory,
) -> None:
    """A duplicate that stands by stops writing (and never deletes) the key."""
    a = await make_node("node_a")
    b = await make_node("node_b", start=False)
    assert await setup_cluster(b.hass, cluster_config("node_a", make_node.url))
    await b.hass.async_block_till_done()
    kv = a.cluster.coordinator.kv_leases
    member_key = a.cluster.leases.member_key

    async def holder() -> str | None:
        try:
            return json_loads_object((await kv.get(member_key)).value)["incarnation"]
        except KeyNotFoundError:
            return None

    await asyncio.sleep(6)
    assert not b.cluster.leases.held
    # Only A writes it now; give A's next renewal time to overwrite B's last.
    await asyncio.sleep(1.5)
    assert await holder() == a.cluster.leases.incarnation
    revision = (await kv.get(member_key)).revision

    await b.cluster.async_release()
    await b.cluster.async_close()
    assert await holder() == a.cluster.leases.incarnation
    assert (await kv.get(member_key)).revision >= revision


async def test_handoff_timer_survives_retries(make_node: NodeFactory) -> None:
    """Retries that take a while do not restart the hand-off timer."""
    rec = make_node.recorder
    b = await make_node("node_b", start=False)
    install_custom(b.hass, "node_b", rec, not_ready=True, setup_delay=3.0)
    await start_node(b, make_node.url)
    first_failure = rec.times("node_b", "setup_failed")[0]
    a = await make_node("node_a")

    await wait_for(lambda: a.entry.state is LOADED, timeout=30)
    assert len(rec.times("node_b", "setup_begin")) >= 2, "no retry ran meanwhile"
    # Without the fix the timer restarts after the first retry (about 18 s).
    took = rec.times("node_a", "setup")[-1] - first_failure
    assert took < 16, f"handed off {took:.1f}s after the first failure"
