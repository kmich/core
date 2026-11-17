"""A real NATS JetStream server and a multi-node in-process cluster.

The tests need a JetStream-enabled NATS server >= 2.15 (the minimum the cluster
accepts) at ``CLUSTER_TEST_NATS_URL``; they are skipped otherwise.
"""

from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
import json
import os
import time
from typing import Any
from unittest.mock import patch

import nats
import pytest
from pytest_homeassistant.common import (
    MockConfigEntry,
    MockModule,
    MockPlatform,
    async_test_home_assistant,
    mock_integration,
    mock_platform,
)

from homeassistant import config_entries
from homeassistant.cluster import DATA_NODE, ClusterNode, async_setup_cluster
from homeassistant.cluster.const import DOMAIN
from homeassistant.cluster.crypto import RecordCipher
from homeassistant.cluster.watchdog import StopWatchdog
from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import (
    area_registry as ar,
    device_registry as dr,
    entity_registry as er,
    issue_registry as ir,
    storage,
)
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from homeassistant.setup import async_setup_component
from homeassistant.util.json import json_loads

from .tcp_proxy import BlackholeProxy

DEV_DOMAIN = "cluster_dev"
DEV_ENTRY_ID = "01JCLUSTERDEVENTRY00000000"
DEV_ENTITY = "switch.dev_switch"


class DevFlow(config_entries.ConfigFlow):
    """Config flow stub so core accepts entries of the fake integration."""

    VERSION = 1

    async def async_step_reauth(self, entry_data: Mapping[str, Any]) -> Any:
        """Ask for nothing; the flow stays open like a real reauthentication."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> Any:
        """Wait for the user."""
        return self.async_show_form(step_id="reauth_confirm")


@pytest.fixture(autouse=True)
def dev_flow_handler() -> Any:
    """Register the stub flow handler for the fake integration."""
    config_entries.HANDLERS[DEV_DOMAIN] = DevFlow
    yield
    config_entries.HANDLERS.pop(DEV_DOMAIN, None)


@dataclass
class HardExit:
    """Records ``os._exit`` calls of the cluster instead of killing pytest."""

    times: list[float] = field(default_factory=list)

    def __call__(self, code: int) -> None:
        """Record when the node would have killed its process."""
        self.times.append(time.monotonic())


@pytest.fixture(autouse=True)
def hard_exit() -> Generator[HardExit]:
    """Replace ``os._exit``: a node that must die is recorded, pytest lives.

    The cluster must look ``os._exit`` up at call time (``os._exit(...)``,
    never a saved reference), or a node that hard-stops ends the test run.
    """
    recorder = HardExit()
    started: list[StopWatchdog] = []
    original_start = StopWatchdog.start

    def _start(watchdog: StopWatchdog) -> None:
        started.append(watchdog)
        original_start(watchdog)

    with (
        patch.object(os, "_exit", recorder),
        patch.object(StopWatchdog, "start", _start),
    ):
        yield recorder
        # A node that stopped while its work still ran keeps its watchdog
        # armed; it must not fire after ``os._exit`` is real again.
        for watchdog in started:
            watchdog.stop()


@pytest.fixture(scope="package")
def nats_url() -> str:
    """URL of a JetStream-enabled NATS server."""
    if not (url := os.environ.get("CLUSTER_TEST_NATS_URL")):
        pytest.skip("set CLUSTER_TEST_NATS_URL to a NATS >= 2.15 server with -js")
    return url


@pytest.fixture
async def clean_nats(nats_url: str, socket_enabled: None) -> str:
    """Delete the cluster buckets so every test starts from an empty cluster."""
    nc = await nats.connect(
        nats_url, connect_timeout=2, max_reconnect_attempts=0, allow_reconnect=False
    )
    js = nc.jetstream()
    for bucket in ("hass_states", "hass_leases", "hass_storage"):
        with suppress(Exception):
            await js.delete_stream(f"KV_{bucket}")
    await nc.close()
    return nats_url


@dataclass
class DevRecorder:
    """Records setup/unload of the fake integration across nodes, in order."""

    events: list[tuple[float, str, str, dict[str, Any]]] = field(default_factory=list)

    def times(self, node: str, kind: str) -> list[float]:
        """Monotonic times of one node's setups or unloads."""
        return [t for t, n, k, _ in self.events if n == node and k == kind]


class DevSwitch(SwitchEntity):
    """A switch whose state only exists on the node running the entry."""

    _attr_should_poll = False

    def __init__(self, entry: ConfigEntry, node: str) -> None:
        """Initialize."""
        self._attr_unique_id = f"{entry.entry_id}_switch"
        self._attr_name = "Dev switch"
        self._attr_is_on = False
        self._attr_extra_state_attributes = {"served_by": node}
        self._attr_device_info = DeviceInfo(identifiers={(DEV_DOMAIN, "dev-1")})

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn on."""
        self._attr_is_on = True
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off."""
        self._attr_is_on = False
        self.async_write_ha_state()


def install_dev_integration(hass: HomeAssistant, node: str, rec: DevRecorder) -> None:
    """Register the fake ``cluster_dev`` integration on one hass instance."""

    async def setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "setup", dict(entry.data)))
        await hass.config_entries.async_forward_entry_setups(entry, ["switch"])

    async def unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "unload", dict(entry.data)))
        await hass.config_entries.async_unload_platforms(entry, ["switch"])

    async def remove_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
        rec.events.append((time.monotonic(), node, "cleanup", dict(entry.data)))

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
            async_unload_entry=unload_entry,
            async_remove_entry=remove_entry,
        ),
    )
    mock_platform(
        hass, f"{DEV_DOMAIN}.switch", MockPlatform(async_setup_entry=switch_setup_entry)
    )
    mock_platform(hass, f"{DEV_DOMAIN}.config_flow", None)


# The key of every test cluster: all nodes of the suite run with encrypted entry records.
TEST_ENCRYPTION_KEY = "test-key-0123456789abcdefghijklmnopqrstuvwxyz"


def open_entry_payload(kv_key: str, raw: bytes) -> dict[str, Any]:
    """The record in ``raw`` with its entry decrypted, as a node reads it."""
    payload = json.loads(raw)
    if "enc" in payload:
        payload = {k: v for k, v in payload.items() if k != "enc"} | {
            "entry": RecordCipher(TEST_ENCRYPTION_KEY).open(kv_key, payload["enc"])
        }
    return payload


def seal_entry_payload(kv_key: str, payload: dict[str, Any]) -> bytes:
    """``payload`` as a leader of the test cluster would publish it."""
    sealed = {k: v for k, v in payload.items() if k != "entry"}
    sealed["enc"] = RecordCipher(TEST_ENCRYPTION_KEY).seal(kv_key, payload["entry"])
    return json.dumps(sealed).encode()


def cluster_config(node_id: str, url: str, **extra: Any) -> dict[str, Any]:
    """YAML configuration of one node."""
    return {
        DOMAIN: {
            "node_id": node_id,
            "nats_url": url,
            "encryption_key": TEST_ENCRYPTION_KEY,
            # The test server has no users; strict mode is tested separately.
            "security": "permissive",
            # Short timers keep the suite fast; tests that depend on the
            # default values pass them explicitly.
            "lease_ttl": 1.5,
            "renew_interval": 0.4,
            "unload_grace": 0.8,
            # A NATS cluster of three servers: CLUSTER_TEST_REPLICAS=3.
            "replicas": int(os.environ.get("CLUSTER_TEST_REPLICAS", "1")),
            **extra,
        }
    }


async def setup_cluster(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    """Set up the cluster of a ``cluster_config`` like bootstrap does."""
    return await async_setup_cluster(hass, config[DOMAIN])


@dataclass
class Node:
    """One cluster member."""

    name: str
    hass: HomeAssistant
    entry: MockConfigEntry

    @property
    def cluster(self) -> ClusterNode:
        """The node's cluster runtime."""
        return self.hass.data[DATA_NODE]

    @property
    def entity_registry(self) -> er.EntityRegistry:
        """The node's entity registry."""
        return er.async_get(self.hass)

    @property
    def device_registry(self) -> dr.DeviceRegistry:
        """The node's device registry."""
        return dr.async_get(self.hass)

    @property
    def area_registry(self) -> ar.AreaRegistry:
        """The node's area registry."""
        return ar.async_get(self.hass)

    @property
    def issue_registry(self) -> ir.IssueRegistry:
        """The node's issue registry."""
        return ir.async_get(self.hass)


type MakeNode = Callable[..., Awaitable[Node]]


def per_node_store_io(
    disks: dict[HomeAssistant, dict[str, Any]],
) -> type[storage._FileStoreIO]:
    """A mocked ``.storage`` of its own for every Home Assistant instance.

    The ``hass_storage`` fixture patches the store class once, so every
    in-process node of a test would share one disk: a node could then "receive"
    what its neighbour saved without the cluster sending it anything. Each node
    gets a dictionary of its own (``disks``); the first one is the fixture's.
    """

    class PerNodeStoreIO(storage._FileStoreIO):
        """Serialized envelopes in the dictionary of this node."""

        def _disk(self) -> dict[str, Any]:
            return disks.setdefault(self._hass, {})

        async def async_load(self) -> Any:
            """Read a copy of this node's envelope."""
            data = self._disk()
            if self._key not in data:
                return {}
            return deepcopy(data[self._key])

        def async_invalidate_cache(self) -> None:
            """The dictionary is authoritative and has no preload cache."""

        async def async_write(self, mode: str, json_data: str | bytes) -> None:
            """Decode the envelope serialized by Store."""
            self._disk()[self._key] = json_loads(json_data)

        async def async_remove(self) -> None:
            """Remove the envelope."""
            self._disk().pop(self._key, None)

    return PerNodeStoreIO


@dataclass
class NodeFactory:
    """Builds nodes; the first one reuses the ``hass`` fixture."""

    hass: HomeAssistant
    url: str
    disks: dict[HomeAssistant, dict[str, Any]] = field(default_factory=dict)
    recorder: DevRecorder = field(default_factory=DevRecorder)
    nodes: list[Node] = field(default_factory=list)
    contexts: list[Any] = field(default_factory=list)

    async def __call__(
        self, name: str, url: str | None = None, *, start: bool = True, **conf: Any
    ) -> Node:
        """Create a node, and start it unless ``start`` is False."""
        if not self.nodes:
            node_hass = self.hass
        else:
            ctx = async_test_home_assistant()
            node_hass = await ctx.__aenter__()
            self.contexts.append(ctx)
        install_dev_integration(node_hass, name, self.recorder)
        entry = MockConfigEntry(
            domain=DEV_DOMAIN, entry_id=DEV_ENTRY_ID, title="Dev", data={"token": "t1"}
        )
        entry.add_to_hass(node_hass)
        node = Node(name, node_hass, entry)
        self.nodes.append(node)
        if start:
            await start_node(node, url or self.url, **conf)
        return node


@pytest.fixture
async def make_node(
    hass: HomeAssistant, clean_nats: str, hass_storage: dict[str, Any]
) -> AsyncGenerator[NodeFactory]:
    """Factory for cluster nodes, each with a ``.storage`` of its own."""
    factory = NodeFactory(hass, clean_nats, {hass: hass_storage})
    with patch(
        "homeassistant.helpers.storage._FileStoreIO", per_node_store_io(factory.disks)
    ):
        yield factory
        for node in factory.nodes:
            if (cluster := node.hass.data.get(DATA_NODE)) is not None:
                await cluster.async_close()
        for node in factory.nodes[1:]:
            await node.hass.async_stop(force=True)
        for ctx in reversed(factory.contexts):
            await ctx.__aexit__(None, None, None)


async def start_node(node: Node, url: str, **conf: Any) -> None:
    """Set up the cluster, then the fake integration (like bootstrap)."""
    assert await setup_cluster(node.hass, cluster_config(node.name, url, **conf))
    assert await async_setup_component(node.hass, DEV_DOMAIN, {})
    await node.hass.async_block_till_done()


@pytest.fixture
async def proxy(nats_url: str, socket_enabled: None) -> AsyncGenerator[BlackholeProxy]:
    """A cuttable link between one node and NATS."""
    host, port = nats_url.removeprefix("nats://").rsplit(":", 1)
    p = BlackholeProxy(host, int(port))
    await p.start()
    yield p
    await p.stop()
