"""Readiness of the cluster: fencing, takeover, forwarded roles, restarts and shutdown."""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
import time
from typing import Any
from unittest.mock import PropertyMock, patch

import nats
import nats.errors
from nats.js.errors import KeyNotFoundError
import pytest
from pytest_homeassistant.common import (
    MockConfigEntry,
    MockModule,
    MockPlatform,
    MockUser,
    mock_integration,
    mock_platform,
)

from homeassistant.auth.const import GROUP_ID_ADMIN, GROUP_ID_READ_ONLY
from homeassistant.cluster.const import DOMAIN
from homeassistant.cluster.coordinator import ClusterCoordinator
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import Context, HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import cluster as cluster_helper, entity_registry as er
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.helpers.service import async_register_admin_service
from homeassistant.helpers.storage import STORAGE_WRITE_LISTENERS
from homeassistant.setup import async_setup_component
from homeassistant.util.json import json_loads_object

from .conftest import (
    DEV_DOMAIN,
    DEV_ENTITY,
    DEV_ENTRY_ID,
    DevRecorder,
    DevSwitch,
    HardExit,
    Node,
    NodeFactory,
    cluster_config,
    setup_cluster,
    start_node,
)
from .tcp_proxy import BlackholeProxy
from .test_failure_modes import install_custom
from .test_init import wait_for

LOADED = ConfigEntryState.LOADED
NOT_LOADED = ConfigEntryState.NOT_LOADED

NEW_ENTRY_ID = "01JCLUSTERDEVENTRYNEW00000"


def install_slow_unload(
    hass: HomeAssistant,
    node: str,
    rec: DevRecorder,
    *,
    unload_time: float,
    release: asyncio.Event,
) -> None:
    """Fake integration whose unload takes ``unload_time`` s, or until released."""

    async def setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "setup", dict(entry.data)))
        await hass.config_entries.async_forward_entry_setups(entry, ["switch"])

    async def unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "unload_begin", {}))
        with suppress(TimeoutError):
            await asyncio.wait_for(release.wait(), unload_time)
        await hass.config_entries.async_unload_platforms(entry, ["switch"])
        rec.events.append((time.monotonic(), node, "unload", {}))

    async def switch_setup_entry(
        hass: HomeAssistant,
        entry: ConfigEntry,
        async_add_entities: AddConfigEntryEntitiesCallback,
    ) -> None:
        async_add_entities([DevSwitch(entry, node)])

    mock_integration(
        hass,
        MockModule(
            DEV_DOMAIN, async_setup_entry=setup_entry, async_unload_entry=unload_entry
        ),
    )
    mock_platform(
        hass, f"{DEV_DOMAIN}.switch", MockPlatform(async_setup_entry=switch_setup_entry)
    )
    mock_platform(hass, f"{DEV_DOMAIN}.config_flow", None)


def failing_storage_put(
    calls: list[str],
) -> Callable[..., Awaitable[int]]:
    """A fenced put (every write to ``hass_storage``) that times out for records."""
    real_put = ClusterCoordinator.async_fenced_put

    async def put(self: ClusterCoordinator, key: str, value: bytes) -> int:
        if not key.startswith(("store.", "entry.")):
            return await real_put(self, key, value)
        calls.append(key)
        await asyncio.sleep(0.01)
        raise nats.errors.TimeoutError

    return put


@pytest.mark.parametrize(
    "unload_time",
    [pytest.param(600.0, id="hung_unload"), pytest.param(8.0, id="slow_unload")],
)
async def test_unload_that_overruns_kills_the_node_before_the_takeover(
    make_node: NodeFactory,
    proxy: BlackholeProxy,
    hard_exit: HardExit,
    unload_time: float,
) -> None:
    """A fenced leader that cannot stop in time dies before the standby starts."""
    rec = make_node.recorder
    release = asyncio.Event()
    a = await make_node("node_a", proxy.url, start=False)
    install_slow_unload(a.hass, "node_a", rec, unload_time=unload_time, release=release)
    await start_node(a, proxy.url)
    b = await make_node("node_b")
    assert a.entry.state is LOADED

    proxy.cut()
    try:
        await wait_for(lambda: b.entry.state is LOADED, timeout=25)
        b_setup = rec.times("node_b", "setup")[-1]
        assert hard_exit.times, "node_a still ran the entry when node_b started it"
        assert hard_exit.times[0] < b_setup
    finally:
        release.set()
        proxy.restore()
        await asyncio.sleep(0.5)


async def test_leadership_expires_even_if_the_fence_callback_is_late(
    make_node: NodeFactory,
) -> None:
    """Work queued before a loop stall must not see a leader after the TTL."""
    a = await make_node("node_a")
    leases = a.cluster.leases
    loop = a.hass.loop
    await asyncio.sleep(1.2)
    seen: list[tuple[bool, bool]] = []

    def probe() -> None:
        seen.append((cluster_helper.async_is_leader(a.hass), leases.is_leader))

    def stall() -> None:
        # A busy loop: time.sleep is refused in the event loop under test.
        end = time.monotonic() + leases.ttl + 1.5
        while time.monotonic() < end:
            pass

    # Due before the fence timer, so it runs first when the loop resumes.
    loop.call_later(0.3, probe)
    loop.call_soon(stall)
    await asyncio.sleep(1)

    assert seen == [(False, False)]


async def test_cold_standby_waits_out_a_leader_it_never_saw(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """A node that boots into an empty lease bucket waits a full lease out."""
    rec = make_node.recorder
    a = await make_node("node_a", proxy.url, lease_ttl=10.0)
    assert a.entry.state is LOADED
    await asyncio.sleep(1.5)  # at least one renewal

    proxy.cut()
    nc = await nats.connect(make_node.url)
    await nc.jetstream().delete_stream("KV_hass_leases")
    await nc.close()
    b = await make_node("node_b", lease_ttl=10.0)
    try:
        await wait_for(lambda: b.entry.state is LOADED, timeout=40)
        a_unloads = rec.times("node_a", "unload")
        assert a_unloads, "node_a still ran the entry when node_b started it"
        assert a_unloads[-1] < rec.times("node_b", "setup")[-1]
    finally:
        proxy.restore()


async def test_forwarded_call_never_exceeds_the_callers_role(
    make_node: NodeFactory,
) -> None:
    """Read-only on the calling node stays read-only on the leader."""
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

    await a.hass.auth.async_create_user("owner")  # the first user is the owner
    sam = await a.hass.auth.async_create_user("sam", group_ids=[GROUP_ID_ADMIN])
    read_only = await b.hass.auth.async_get_group(GROUP_ID_READ_ONLY)
    assert read_only is not None
    MockUser(id=sam.id, name="sam", groups=[read_only]).add_to_hass(b.hass)

    with pytest.raises(HomeAssistantError):
        await b.hass.services.async_call(
            DEV_DOMAIN, "factory_reset", blocking=True, context=Context(user_id=sam.id)
        )
    assert admin_calls == []


async def test_forwarded_call_runs_as_the_role_user(
    make_node: NodeFactory,
) -> None:
    """A forwarded call runs as the leader's user of the caller's role.

    Users exist per node, so even a user id the leader knows is not taken
    over; an internal call (no user) stays internal.
    """
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

    await a.hass.auth.async_create_user("owner")  # the first user is the owner
    sam = await a.hass.auth.async_create_user("sam", group_ids=[GROUP_ID_ADMIN])
    admin = await b.hass.auth.async_get_group(GROUP_ID_ADMIN)
    assert admin is not None
    MockUser(id=sam.id, name="sam", groups=[admin]).add_to_hass(b.hass)

    await b.hass.services.async_call(
        DEV_DOMAIN, "factory_reset", blocking=True, context=Context(user_id=sam.id)
    )
    await b.hass.services.async_call(DEV_DOMAIN, "factory_reset", blocking=True)

    forwarded, internal = (call.context.user_id for call in admin_calls)
    assert internal is None
    assert forwarded not in (None, sam.id)
    user = await a.hass.auth.async_get_user(forwarded)
    assert user is not None
    assert user.system_generated
    assert user.is_admin


async def test_restart_inside_the_ttl_after_a_crash_leads_again(
    make_node: NodeFactory,
) -> None:
    """The only node dies without releasing and is back before its keys expire."""
    a = await make_node("node_a")
    assert a.entry.state is LOADED
    await a.cluster.leases.async_stop(release=False)  # crash: nothing released

    restarted = await make_node("node_b", start=False)
    assert await setup_cluster(restarted.hass, cluster_config("node_a", make_node.url))
    assert await async_setup_component(restarted.hass, DEV_DOMAIN, {})
    await restarted.hass.async_block_till_done()

    await wait_for(lambda: restarted.entry.state is LOADED, timeout=30)
    assert cluster_helper.async_is_leader(restarted.hass)


async def test_duplicate_takes_over_once_the_original_is_gone(
    make_node: NodeFactory,
) -> None:
    """A rolling restart; the old process stops after the new one booted."""
    later = await make_node("node_b", start=False)
    original = await make_node("node_a")
    assert original.entry.state is LOADED
    assert await setup_cluster(later.hass, cluster_config("node_a", make_node.url))
    assert await async_setup_component(later.hass, DEV_DOMAIN, {})
    await later.hass.async_block_till_done()
    await asyncio.sleep(3)
    assert (original.entry.state, later.entry.state) == (LOADED, NOT_LOADED)

    await original.hass.async_stop()

    await wait_for(lambda: later.entry.state is LOADED, timeout=40)
    assert cluster_helper.async_is_leader(later.hass)


async def test_standby_rejoins_the_members_after_a_long_partition(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """A standby cut off for longer than the TTL is a member again."""
    a = await make_node("node_a")
    b = await make_node("node_b", proxy.url)
    await wait_for(lambda: "node_b" in a.cluster.leases.members, timeout=5)

    proxy.cut()
    await asyncio.sleep(a.cluster.leases.ttl * 2)
    proxy.restore()
    await wait_for(lambda: b.cluster.leases.leader() == "node_a", timeout=30)

    await wait_for(lambda: "node_b" in a.cluster.leases.members, timeout=15)


async def test_flush_fails_while_a_store_write_is_unpublished(
    make_node: NodeFactory,
) -> None:
    """The shutdown flush must not succeed over a store it could not publish."""
    a = await make_node("node_a")
    storage = a.cluster.storage
    calls: list[str] = []

    with patch.object(
        ClusterCoordinator, "async_fenced_put", failing_storage_put(calls)
    ):
        # What Core's store write hook delivers after a store was saved.
        for listener, _ in a.hass.data[STORAGE_WRITE_LISTENERS]:
            listener("core.label_registry", '{"v": 1}')
            # Not a store a standby reads: never published.
            listener("cluster_readiness", '{"v": 1}')
        await asyncio.sleep(0.5)
        assert "store.core.label_registry" in calls
        assert "store.cluster_readiness" not in calls
        with pytest.raises(HomeAssistantError):
            await asyncio.wait_for(storage.async_flush(), 15)

    await asyncio.wait_for(storage.async_flush(), 15)
    kv = a.cluster.coordinator.kv_storage
    assert b'"v": 1' in (await kv.get("store.core.label_registry")).value
    with pytest.raises(KeyNotFoundError):
        await kv.get("store.cluster_readiness")


async def test_failing_entry_publication_backs_off_and_flush_gives_up(
    make_node: NodeFactory,
) -> None:
    """A publication that keeps failing is retried slowly; flush raises."""
    a = await make_node("node_a")
    calls: list[str] = []

    with patch.object(
        ClusterCoordinator, "async_fenced_put", failing_storage_put(calls)
    ):
        a.hass.config_entries.async_update_entry(a.entry, data={"token": "t2"})
        await asyncio.sleep(2)
        attempts = calls.count(f"entry.{DEV_ENTRY_ID}")
        with pytest.raises(HomeAssistantError):
            await asyncio.wait_for(a.cluster.storage.async_flush(), 15)

    assert 1 <= attempts <= 6, f"{attempts} attempts in 2 s"


async def test_setup_refused_while_reconnecting_is_retried(
    make_node: NodeFactory,
) -> None:
    """The leader reloads an entry during a NATS blip; it must come back."""
    a = await make_node("node_a")
    assert a.entry.state is LOADED

    with patch.object(
        ClusterCoordinator, "connected", new_callable=PropertyMock, return_value=False
    ):
        await a.hass.config_entries.async_reload(DEV_ENTRY_ID)

    await wait_for(lambda: a.entry.state is LOADED, timeout=10)
    assert a.cluster.leases.is_leader


async def wait_for_mirrored_store(
    node: Node,
    store_key: str,
    predicate: Callable[[dict[str, Any]], bool],
    timeout: float = 30.0,
) -> None:
    """Wait until the store that ``node`` mirrored satisfies ``predicate``.

    Registries are saved up to 10 s after a change, and mirrored then.
    """
    kv = node.cluster.coordinator.kv_storage
    assert kv is not None
    deadline = time.monotonic() + timeout
    while True:
        try:
            envelope = json_loads_object((await kv.get(f"store.{store_key}")).value)
        except KeyNotFoundError:
            envelope = {}
        content = envelope.get("content")
        if isinstance(content, dict) and predicate(content["data"]):
            return
        assert time.monotonic() < deadline, f"{store_key} was never mirrored"
        await asyncio.sleep(0.2)


async def fail_over_gracefully(a_hass: HomeAssistant) -> None:
    """Unload the dev entries on the leader, publish and release the lease."""
    for entry in a_hass.config_entries.async_entries(DEV_DOMAIN):
        await a_hass.config_entries.async_unload(entry.entry_id)
    await a_hass.data[DOMAIN].async_release()


async def test_entry_added_on_the_leader_survives_failover(
    make_node: NodeFactory,
) -> None:
    """An integration added on the leader exists on the next leader."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    new = MockConfigEntry(domain=DEV_DOMAIN, entry_id=NEW_ENTRY_ID, title="New")
    await a.hass.config_entries.async_add(new)
    await a.hass.async_block_till_done()
    assert new.state is LOADED
    await asyncio.sleep(2)

    await fail_over_gracefully(a.hass)
    await wait_for(lambda: b.entry.state is LOADED, timeout=10)
    await asyncio.sleep(1)

    b_entry = b.hass.config_entries.async_get_entry(NEW_ENTRY_ID)
    assert b_entry is not None
    assert b_entry.state is LOADED


async def test_entity_renamed_on_the_leader_survives_failover(
    make_node: NodeFactory, entity_registry: er.EntityRegistry
) -> None:
    """An entity id changed on the leader is the id the next leader uses."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    entity_registry.async_update_entity(DEV_ENTITY, new_entity_id="switch.kitchen")
    await a.hass.async_block_till_done()
    await wait_for(lambda: b.hass.states.get("switch.kitchen") is not None)
    await wait_for_mirrored_store(
        a,
        "core.entity_registry",
        lambda data: any(e["entity_id"] == "switch.kitchen" for e in data["entities"]),
    )

    await fail_over_gracefully(a.hass)
    await wait_for(lambda: b.entry.state is LOADED, timeout=10)
    await asyncio.sleep(1)

    state = b.hass.states.get("switch.kitchen")
    assert state is not None
    assert state.attributes.get("served_by") == "node_b"
    assert b.hass.states.get(DEV_ENTITY) is None


async def test_yaml_thermostat_on_the_standby_does_not_switch_the_heater(
    make_node: NodeFactory,
) -> None:
    """The standby's copy of a YAML thermostat must not act on the house."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    conf = {
        "climate": {
            "platform": "generic_thermostat",
            "name": "room",
            "heater": DEV_ENTITY,
            "target_sensor": "sensor.room_temp",
            "target_temp": 20,
            "initial_hvac_mode": "heat",
        }
    }
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is not None)
    for node in (a, b):
        node.hass.states.async_set("sensor.room_temp", "16")
        assert await async_setup_component(node.hass, "homeassistant", {})
        assert await async_setup_component(node.hass, "climate", conf)
        await node.hass.async_block_till_done()
    a.hass.states.async_set("sensor.room_temp", "15")
    await wait_for(lambda: a.hass.states.get(DEV_ENTITY).state == "on", timeout=5)
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY).state == "on", timeout=5)

    # The user turns the heating off from the standby's UI.
    await b.hass.services.async_call(
        "climate",
        "set_hvac_mode",
        {"entity_id": "climate.room", "hvac_mode": "off"},
        blocking=True,
    )
    await wait_for(lambda: a.hass.states.get(DEV_ENTITY).state == "off", timeout=5)
    # The standby's thermostat only acts once it sees the heater off.
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY).state == "off", timeout=5)
    assert a.hass.states.get("climate.room").state == "off"

    # The next temperature reading reaches the standby.
    b.hass.states.async_set("sensor.room_temp", "14")
    await asyncio.sleep(2)

    assert a.hass.states.get(DEV_ENTITY).state == "off"
