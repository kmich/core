"""YAML platforms run on the leader only."""

from typing import Any

from pytest_homeassistant.common import (
    MockModule,
    MockPlatform,
    mock_integration,
    mock_platform,
)

from homeassistant.components.sensor import SensorEntity
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from homeassistant.setup import async_setup_component

from .conftest import Node, NodeFactory
from .test_init import wait_for
from .test_readiness import fail_over_gracefully

YAML_DOMAIN = "cluster_yaml"
PROBE = "sensor.yaml_probe"
SENSOR_CONFIG = {"sensor": [{"platform": YAML_DOMAIN}]}


class YamlProbe(SensorEntity):
    """A YAML sensor that tells which node's entity object is behind its state."""

    _attr_should_poll = False
    _attr_name = "YAML probe"
    _attr_native_value = "ready"

    def __init__(self, node: str) -> None:
        """Initialize."""
        self._attr_extra_state_attributes = {"served_by": node}


def install_yaml_platform(hass: HomeAssistant, node: str, setups: list[str]) -> None:
    """Register a fake integration with a YAML sensor platform on one node."""

    async def setup_platform(
        hass: HomeAssistant,
        config: ConfigType,
        async_add_entities: AddEntitiesCallback,
        discovery_info: DiscoveryInfoType | None = None,
    ) -> None:
        setups.append(node)
        async_add_entities([YamlProbe(node)])

    mock_integration(hass, MockModule(YAML_DOMAIN))
    mock_platform(
        hass, f"{YAML_DOMAIN}.sensor", MockPlatform(async_setup_platform=setup_platform)
    )


def served_by(hass: HomeAssistant) -> Any:
    """Which node's entity object stands behind the probe's state here."""
    state = hass.states.get(PROBE)
    return None if state is None else state.attributes.get("served_by")


async def start_yaml_platform(nodes: tuple[Node, ...], setups: list[str]) -> None:
    """Configure the YAML sensor platform on every node, as shared YAML would."""
    for node in nodes:
        install_yaml_platform(node.hass, node.name, setups)
        assert await async_setup_component(node.hass, "sensor", SENSOR_CONFIG)
        await node.hass.async_block_till_done()


async def test_yaml_platform_runs_on_the_leader_only(make_node: NodeFactory) -> None:
    """The standby does not set the platform up; it mirrors the leader's."""
    setups: list[str] = []
    a = await make_node("node_a")
    b = await make_node("node_b")
    await start_yaml_platform((a, b), setups)

    assert served_by(a.hass) == "node_a"
    await wait_for(lambda: served_by(b.hass) == "node_a", timeout=10)
    assert PROBE in b.cluster.replicator.mirrored
    assert setups == ["node_a"]


async def test_yaml_platform_moves_with_leadership(make_node: NodeFactory) -> None:
    """The new leader sets the platform up; the old leader removes it."""
    setups: list[str] = []
    a = await make_node("node_a")
    b = await make_node("node_b")
    await start_yaml_platform((a, b), setups)
    await wait_for(lambda: served_by(b.hass) == "node_a", timeout=10)

    await fail_over_gracefully(a.hass)

    await wait_for(lambda: served_by(b.hass) == "node_b", timeout=15)
    await wait_for(lambda: served_by(a.hass) != "node_a", timeout=10)
    assert setups == ["node_a", "node_b"]


async def test_yaml_platform_of_an_excluded_domain_runs_on_every_node(
    make_node: NodeFactory,
) -> None:
    """A domain that runs on every node keeps its YAML platforms everywhere."""
    setups: list[str] = []
    a = await make_node("node_a", exclude_domains=[YAML_DOMAIN])
    b = await make_node("node_b", exclude_domains=[YAML_DOMAIN])
    await start_yaml_platform((a, b), setups)

    assert (served_by(a.hass), served_by(b.hass)) == ("node_a", "node_b")
    assert PROBE not in b.cluster.replicator.mirrored
    assert sorted(setups) == ["node_a", "node_b"]
