"""Load the leader's registries when this node takes over.

The leader mirrors every registry store to ``store.<key>`` in the
``hass_storage`` bucket (``storage_sync``). A standby never applies them while
it runs: its in-memory registries would overwrite the file. At takeover, after
the config entries were reconciled and before any entry is set up, this node
loads them (``BaseRegistry.async_replace``), so that the ids that automations
and dashboards refer to (entity registry ids and entity ids, device ids, areas,
labels) are the ones the old leader used.

Floors, areas, labels and categories are taken as they are. Devices and
entities are merged: what belongs to a config entry that is replicated (a
clustered domain) comes from the leader; what does not (YAML and per-node
entities, entries of integrations that run on every node) stays this node's.
"""

from collections.abc import Callable
import logging
from typing import Any, cast

from nats.js.errors import KeyDeletedError, KeyNotFoundError

from homeassistant.core import HomeAssistant
from homeassistant.helpers import (
    area_registry as ar,
    category_registry as cr,
    cluster as cluster_helper,
    device_registry as dr,
    entity_registry as er,
    floor_registry as fr,
    label_registry as lr,
)
from homeassistant.helpers.registry import BaseRegistry
from homeassistant.util.json import json_loads

from .const import STORAGE_KEY_PREFIX
from .lease_manager import DATA_ERRORS, HandoverIncomplete, LeaseManager

_LOGGER = logging.getLogger(__name__)


def _identifiers(item: dict[str, Any], key: str) -> set[tuple[Any, ...]]:
    return {tuple(value) for value in item.get(key, [])}


def _device_clash(local: dict[str, Any], taken: dict[str, Any]) -> bool:
    return (
        local["id"] == taken["id"]
        or bool(_identifiers(local, "identifiers") & _identifiers(taken, "identifiers"))
        or bool(_identifiers(local, "connections") & _identifiers(taken, "connections"))
    )


def _entity_clash(local: dict[str, Any], taken: dict[str, Any]) -> bool:
    return (
        local["entity_id"] == taken["entity_id"]
        or local["id"] == taken["id"]
        or (
            local["platform"],
            local["unique_id"],
            local["entity_id"].partition(".")[0],
        )
        == (
            taken["platform"],
            taken["unique_id"],
            taken["entity_id"].partition(".")[0],
        )
    )


def _merge_items(
    leader_items: list[dict[str, Any]],
    local_items: list[dict[str, Any]],
    *,
    belongs: Callable[[dict[str, Any]], bool],
    clash: Callable[[dict[str, Any], dict[str, Any]], bool],
    name: str,
) -> list[dict[str, Any]]:
    """Take what belongs to the leader from it, keep this node's own items."""
    taken = [item for item in leader_items if belongs(item)]
    kept = []
    for item in local_items:
        if belongs(item):
            continue  # the leader's copy replaces it
        if any(clash(item, other) for other in taken):
            _LOGGER.info("The leader's %s replaces a local one: %s", name, item["id"])
            continue
        kept.append(item)
    return taken + kept


def merge_device_data(
    leader: dict[str, Any], local: dict[str, Any], replicated: set[str]
) -> dict[str, Any]:
    """Merge the leader's device registry data with this node's.

    ``replicated`` is the set of ids of the config entries the leader's records
    are taken for. Child devices follow the same rule as devices; one whose
    parent did not make it into the result is dropped.
    """

    def belongs(item: dict[str, Any]) -> bool:
        return item.get("config_entry_id") in replicated

    merged = dict(leader)
    for key in ("devices", "deleted_devices", "child_devices"):
        merged[key] = _merge_items(
            leader.get(key, []),
            local.get(key, []),
            belongs=belongs,
            clash=_device_clash,
            name="device",
        )
    device_ids = {device["id"] for device in merged["devices"]}
    merged["child_devices"] = [
        child
        for child in merged["child_devices"]
        if child["parent_device_id"] in device_ids
    ]
    return merged


def merge_entity_data(
    leader: dict[str, Any],
    local: dict[str, Any],
    replicated: set[str],
    gated_platform: Callable[[str], bool] = lambda platform: False,
) -> dict[str, Any]:
    """Merge the leader's entity registry data with this node's.

    Entities without a config entry (YAML, per node) stay this node's own,
    except those of a gated YAML or discovered platform (``gated_platform``
    says which platform names run on the leader only): the platform name
    differs from the domain of the entity id. They belong to the leader.
    """

    def belongs(item: dict[str, Any]) -> bool:
        if (entry_id := item.get("config_entry_id")) is not None:
            return entry_id in replicated
        platform = item.get("platform")
        return (
            platform is not None
            and platform != item["entity_id"].partition(".")[0]
            and gated_platform(platform)
        )

    merged = dict(leader)
    for key in ("entities", "deleted_entities"):
        merged[key] = _merge_items(
            leader.get(key, []),
            local.get(key, []),
            belongs=belongs,
            clash=_entity_clash,
            name="entity",
        )
    return merged


async def async_load_registries(
    hass: HomeAssistant,
    node_id: str,
    kv: Any,
    leases: LeaseManager,
) -> None:
    """Load the leader's mirrored registries into this node.

    Raises ``HandoverIncomplete`` if a registry cannot be read.
    """
    replicated = {
        entry.entry_id
        for entry in hass.config_entries.async_entries()
        if leases.domain_is_clustered(entry.domain)
    }
    registries: list[tuple[str, BaseRegistry[Any]]] = [
        ("core.floor_registry", fr.async_get(hass)),
        ("core.area_registry", ar.async_get(hass)),
        ("core.label_registry", lr.async_get(hass)),
        ("core.category_registry", cr.async_get(hass)),
        ("core.device_registry", dr.async_get(hass)),
        ("core.entity_registry", er.async_get(hass)),
    ]
    for key, registry in registries:
        data = await _async_read(kv, node_id, key, registry)
        if data is None:
            continue
        try:
            if key == "core.device_registry":
                data = merge_device_data(data, registry.async_dump(), replicated)
            elif key == "core.entity_registry":
                data = merge_entity_data(
                    data,
                    registry.async_dump(),
                    replicated,
                    lambda platform: (
                        not cluster_helper.async_domain_runs_on_every_instance(
                            hass, platform
                        )
                    ),
                )
        except DATA_ERRORS:
            # Its content is wrong on every retry, and nothing was changed yet:
            # keep this node's, loudly.
            _LOGGER.exception(
                "Not loading the leader's %s: its content is invalid; keeping"
                " this node's",
                key,
            )
            continue
        try:
            await registry.async_replace(data)
        except Exception as err:
            # Whatever failed here may have changed the registry already:
            # running with a mix of the leader's and this node's registries is
            # worse than not leading yet, so the promotion is tried again.
            raise HandoverIncomplete(
                f"could not load the leader's {key}: {err!r}"
            ) from err


async def _async_read(
    kv: Any, node_id: str, key: str, registry: BaseRegistry[Any]
) -> dict[str, Any] | None:
    """Return the data of a mirrored registry, or None to keep this node's."""
    try:
        current = await kv.get(f"{STORAGE_KEY_PREFIX}{key}")
    except KeyNotFoundError, KeyDeletedError:
        _LOGGER.warning(
            "The leader's %s is not in the cluster (nothing was mirrored yet, or"
            " it is larger than the NATS max_payload); keeping this node's",
            key,
        )
        return None
    except Exception as err:
        raise HandoverIncomplete(f"could not read {key}: {err!r}") from err
    try:
        envelope: dict[str, Any] = cast(
            dict[str, Any], json_loads(current.value or b"{}")
        )
        if envelope.get("origin") == node_id:
            return None
        content: dict[str, Any] = envelope["content"]
        version = (content["version"], content["minor_version"])
        data: dict[str, Any] = content["data"]
    except ValueError, KeyError, TypeError, AttributeError:
        _LOGGER.warning("Ignoring the malformed mirror of %s", key)
        return None
    store = registry._store  # noqa: SLF001
    if version != (store.version, store.minor_version):
        _LOGGER.warning(
            "Not loading the leader's %s: it has version %s.%s, this node %s.%s;"
            " nodes on different versions cannot share registries",
            key,
            *version,
            store.version,
            store.minor_version,
        )
        return None
    return data
