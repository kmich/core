"""The leader's registries are loaded at takeover."""

from typing import Any

from homeassistant.config_entries import ConfigEntryState

from .conftest import DEV_DOMAIN, DEV_ENTITY, DEV_ENTRY_ID, NodeFactory
from .test_init import wait_for
from .test_readiness import fail_over_gracefully, wait_for_mirrored_store

LOADED = ConfigEntryState.LOADED

KITCHEN = "switch.kitchen"


def _has_entity(entity_id: str, **fields: Any) -> Any:
    """Predicate: the mirrored entity registry has this entity with these fields."""

    def check(data: dict[str, Any]) -> bool:
        return any(
            e["entity_id"] == entity_id and all(e[k] == v for k, v in fields.items())
            for e in data["entities"]
        )

    return check


async def test_entity_and_registry_ids_follow_the_leader(
    make_node: NodeFactory,
) -> None:
    """The next leader uses the leader's entity id, name and registry id."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    er_a = a.entity_registry
    er_a.async_update_entity(DEV_ENTITY, new_entity_id=KITCHEN, name="Kettle")
    await wait_for_mirrored_store(a, "core.entity_registry", _has_entity(KITCHEN))
    leader_entry = er_a.async_get(KITCHEN)
    assert leader_entry is not None

    await fail_over_gracefully(a.hass)
    await wait_for(lambda: b.entry.state is LOADED, timeout=10)

    er_b = b.entity_registry
    entry = er_b.async_get(KITCHEN)
    assert entry is not None
    assert (entry.id, entry.name) == (leader_entry.id, "Kettle")
    assert entry.config_entry_id == DEV_ENTRY_ID
    assert er_b.async_get(DEV_ENTITY) is None


async def test_area_assignment_follows_the_leader(make_node: NodeFactory) -> None:
    """An area made on the leader, and its entity, exist on the next leader."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    area = a.area_registry.async_create("Kitchen")
    a.entity_registry.async_update_entity(DEV_ENTITY, area_id=area.id)
    await wait_for_mirrored_store(
        a,
        "core.area_registry",
        lambda data: any(x["id"] == area.id for x in data["areas"]),
    )
    await wait_for_mirrored_store(
        a, "core.entity_registry", _has_entity(DEV_ENTITY, area_id=area.id)
    )

    await fail_over_gracefully(a.hass)
    await wait_for(lambda: b.entry.state is LOADED, timeout=10)

    b_area = b.area_registry.async_get_area(area.id)
    assert b_area is not None
    assert b_area.name == "Kitchen"
    entry = b.entity_registry.async_get(DEV_ENTITY)
    assert entry is not None
    assert entry.area_id == area.id


async def test_device_ids_follow_the_leader(make_node: NodeFactory) -> None:
    """A device keeps the id that automations refer to."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    device = a.device_registry.async_get_or_create(
        config_entry_id=DEV_ENTRY_ID,
        identifiers={(DEV_DOMAIN, "dev-1")},
        name="Dev box",
    )
    a.entity_registry.async_update_entity(DEV_ENTITY, device_id=device.id)
    await wait_for_mirrored_store(
        a,
        "core.device_registry",
        lambda data: any(d["id"] == device.id for d in data["devices"]),
    )
    await wait_for_mirrored_store(
        a, "core.entity_registry", _has_entity(DEV_ENTITY, device_id=device.id)
    )

    await fail_over_gracefully(a.hass)
    await wait_for(lambda: b.entry.state is LOADED, timeout=10)

    b_device = b.device_registry.async_get(device.id)
    assert b_device is not None
    assert b_device.identifiers == {(DEV_DOMAIN, "dev-1")}
    assert b_device.name == "Dev box"
    entry = b.entity_registry.async_get(DEV_ENTITY)
    assert entry is not None
    assert entry.device_id == device.id


async def test_standby_keeps_its_own_registry_entries(make_node: NodeFactory) -> None:
    """Entities that are this node's own survive loading the leader's.

    The entity's platform runs on every node (``exclude_domains``): the
    entity of a platform that runs on the leader only belongs to the leader
    """
    a = await make_node("node_a", exclude_domains=["node_local"])
    b = await make_node("node_b", exclude_domains=["node_local"])
    er_b = b.entity_registry
    own = er_b.async_get_or_create(
        "sensor", "node_local", "local-1", suggested_object_id="local_probe"
    )
    a.entity_registry.async_update_entity(DEV_ENTITY, new_entity_id=KITCHEN)
    await wait_for_mirrored_store(a, "core.entity_registry", _has_entity(KITCHEN))

    await fail_over_gracefully(a.hass)
    await wait_for(lambda: b.entry.state is LOADED, timeout=10)

    assert er_b.async_get(KITCHEN) is not None
    kept = er_b.async_get(own.entity_id)
    assert kept is not None
    assert (kept.id, kept.unique_id, kept.platform) == (own.id, "local-1", "node_local")
