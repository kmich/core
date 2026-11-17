"""Unit tests for merging the leader's registries with this node's."""

from typing import Any

from homeassistant.cluster.registry_sync import merge_device_data, merge_entity_data

REPLICATED = {"entry-1"}


def entity(
    entity_id: str, id_: str, config_entry_id: str | None, unique_id: str = "u"
) -> dict[str, Any]:
    """An entity registry item as stored."""
    return {
        "entity_id": entity_id,
        "id": id_,
        "config_entry_id": config_entry_id,
        "platform": "test",
        "unique_id": unique_id,
    }


def device(
    id_: str,
    config_entry_id: str | None,
    identifiers: list[list[str]],
    connections: list[list[str]] | None = None,
) -> dict[str, Any]:
    """A device registry item as stored."""
    return {
        "id": id_,
        "config_entry_id": config_entry_id,
        "identifiers": identifiers,
        "connections": connections or [],
    }


def test_entities_of_replicated_entries_come_from_the_leader() -> None:
    """The leader's entities replace this node's; its own entities stay."""
    leader = {
        "entities": [
            entity("switch.kitchen", "L1", "entry-1", "u1"),
            entity("sensor.other", "L2", "entry-2", "u2"),
        ],
        "deleted_entities": [entity("light.gone", "L3", "entry-1", "u3")],
        "settings": {"entity_id_parts": ["leader"]},
    }
    local = {
        "entities": [
            entity("switch.dev", "N1", "entry-1", "u1"),
            entity("sensor.local", "N2", None, "local-1"),
            entity("sensor.node_entry", "N3", "entry-2", "u4"),
            # Clashes only with an entity of an entry that is not replicated.
            entity("sensor.clash", "N4", None, "u2"),
        ],
        "deleted_entities": [entity("light.old", "N5", "entry-1", "u9")],
        "settings": {"entity_id_parts": ["local"]},
    }

    merged = merge_entity_data(leader, local, REPLICATED)

    assert sorted(e["id"] for e in merged["entities"]) == ["L1", "N2", "N3", "N4"]
    assert [e["id"] for e in merged["deleted_entities"]] == ["L3"]
    assert merged["settings"] == {"entity_id_parts": ["leader"]}


def test_a_local_entity_with_the_id_of_a_taken_one_is_dropped() -> None:
    """The leader wins on a clash of entity id, registry id or unique id."""
    leader = {
        "entities": [entity("switch.kitchen", "L1", "entry-1", "u1")],
        "deleted_entities": [],
        "settings": {},
    }
    local = {
        "entities": [
            entity("switch.kitchen", "N1", None, "a"),
            entity("sensor.x", "L1", None, "b"),
            entity("switch.y", "N3", None, "u1"),
            entity("sensor.z", "N4", None, "u1"),
        ],
        "deleted_entities": [],
        "settings": {},
    }

    merged = merge_entity_data(leader, local, REPLICATED)

    # ``switch.y`` has the unique id of the taken entity, in the same domain.
    assert sorted(e["id"] for e in merged["entities"]) == ["L1", "N4"]


def test_devices_follow_the_same_rule_and_children_need_their_parent() -> None:
    """Devices of replicated entries come from the leader; orphans are dropped."""
    leader = {
        "devices": [device("LD", "entry-1", [["test", "1"]])],
        "deleted_devices": [],
        "child_devices": [
            {"id": "LC", "config_entry_id": "entry-1", "parent_device_id": "LD"},
            {"id": "LX", "config_entry_id": "entry-1", "parent_device_id": "ND9"},
        ],
    }
    local = {
        "devices": [
            device("ND1", "entry-1", [["test", "1"]]),
            device("ND2", None, [["own", "2"]]),
            device("ND3", None, [["test", "1"]]),
        ],
        "deleted_devices": [],
        "child_devices": [],
    }

    merged = merge_device_data(leader, local, REPLICATED)

    assert sorted(d["id"] for d in merged["devices"]) == ["LD", "ND2"]
    # ``LX`` is the leader's, but its parent is not in the result.
    assert [c["id"] for c in merged["child_devices"]] == ["LC"]
