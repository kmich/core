"""Follow what is made in the leader's UI: automations, scripts, scenes, helpers.

The config editor writes automations, scripts and scenes to three files in the
configuration directory, and the UI helpers (``input_boolean``, ``counter``,
``timer``, ``person``, ...) are storage collections; neither is a config
entry or a registry. The leader publishes them to
the ``hass_storage`` bucket and every standby applies them:

``ui.file.<file name>``
    The text of ``automations.yaml``, ``scripts.yaml`` or ``scenes.yaml``.
    The leader checks the files every second. A standby writes the file and
    reloads the domain.

``ui.collection.<store key>``
    The items of a helper collection in their storage form, published as
    soon as the collection changes. A standby replaces its items with them
    (``StorageCollection.async_replace``): ids stay, entities follow.

``ui_state.<domain>.<item id>``
    The last state of a helper entity, published on every state change. Read
    only when a standby is promoted: it creates the helper entities again,
    and they restore the leader's state as after a restart.

A standby applies definitions live and in full before a promotion; the node
that holds the lease never applies.
"""

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
import hashlib
import logging
import os
import time
from typing import TYPE_CHECKING, Any

from nats.js.errors import KeyDeletedError, KeyNotFoundError, NoKeysError

from homeassistant.config import (
    AUTOMATION_CONFIG_PATH,
    SCENE_CONFIG_PATH,
    SCRIPT_CONFIG_PATH,
)
from homeassistant.const import (
    EVENT_COMPONENT_LOADED,
    EVENT_STATE_CHANGED,
    SERVICE_RELOAD,
)
from homeassistant.core import (
    CALLBACK_TYPE,
    Event,
    EventStateChangedData,
    HomeAssistant,
    State,
    callback,
)
from homeassistant.helpers import entity_registry as er, restore_state
from homeassistant.helpers.collection import (
    CollectionChange,
    CollectionEntity,
    StorageCollection,
    async_get_storage_collection,
)
from homeassistant.helpers.entity_component import DATA_INSTANCES
from homeassistant.helpers.json import json_bytes
from homeassistant.helpers.restore_state import RestoreEntity, StoredState
from homeassistant.util import dt as dt_util
from homeassistant.util.file import write_utf8_file_atomic
from homeassistant.util.json import json_loads_object
from homeassistant.util.yaml import parse_yaml

from .const import LEADER_KEY
from .coordinator import ClusterCoordinator, TermSuperseded, stop_watcher
from .lease_manager import DATA_ERRORS, ApplyFailed, HandoverIncomplete, LeaseManager

_LOGGER = logging.getLogger(__name__)

UI_KEY_PREFIX = "ui."
UI_FILE_KEY_PREFIX = "ui.file."
UI_COLLECTION_KEY_PREFIX = "ui.collection."
UI_STATE_KEY_PREFIX = "ui_state."

# The files the config editor writes, and the domain that reads each.
UI_CONFIG_FILES = {
    AUTOMATION_CONFIG_PATH: "automation",
    SCRIPT_CONFIG_PATH: "script",
    SCENE_CONFIG_PATH: "scene",
}
# Entity registry platform of the entities made from each file.
_REGISTRY_PLATFORMS = {
    "automation": "automation",
    "script": "script",
    "scene": "homeassistant",
}
# Helpers made in the UI: storage collections whose store key is the domain.
UI_HELPER_DOMAINS = frozenset(
    {
        "counter",
        "input_boolean",
        "input_button",
        "input_datetime",
        "input_number",
        "input_select",
        "input_text",
        "person",
        "schedule",
        "tag",
        "timer",
        "zone",
    }
)

_POLL_INTERVAL = 1.0


def _valid_kv_key(key: str) -> bool:
    return bool(key) and all(c.isalnum() or c in "-/_=." for c in key)


def _digest(value: Any) -> bytes:
    return hashlib.sha256(json_bytes(value)).digest()


def _stat(path: str) -> tuple[int, int] | None:
    try:
        result = os.stat(path)
    except FileNotFoundError:
        return None
    return (result.st_mtime_ns, result.st_size)


def _read_text(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError:
        return None


def _config_ids(domain: str, content: str | None) -> set[str] | None:
    """Ids of the items in a UI file, or None if it cannot be parsed."""
    if content is None:
        return set()
    try:
        data = parse_yaml(content)
    except Exception:  # noqa: BLE001 - e.g. a tag that needs another file
        return None
    if not data:
        return set()
    if domain == "script":
        return {str(key) for key in data} if isinstance(data, dict) else None
    if not isinstance(data, list):
        return None
    return {str(item["id"]) for item in data if isinstance(item, dict) and "id" in item}


def _replace_file(
    path: str, domain: str, content: str | None
) -> tuple[set[str] | None, set[str] | None] | None:
    """Write ``content`` to ``path`` unless it is there already.

    ``content`` None deletes the file (the leader deleted it). Returns the
    item ids before and after (each None if it cannot be parsed), or None if
    nothing was written. One executor job, so that the file cannot change
    between reading and writing it here.
    """
    local = _read_text(path)
    if local == content:
        return None
    old_ids = _config_ids(domain, local)
    new_ids = _config_ids(domain, content)
    if content is None:
        with suppress(FileNotFoundError):
            os.remove(path)
    else:
        write_utf8_file_atomic(path, content)
    return old_ids, new_ids


class UiConfigSync:
    """Publishes the leader's UI configuration and applies it on standbys."""

    def __init__(
        self,
        hass: HomeAssistant,
        node_id: str,
        coordinator: ClusterCoordinator,
        lease_manager: LeaseManager,
        *,
        wakeup: Callable[[], None],
    ) -> None:
        """Initialize; ``wakeup`` starts the storage writer."""
        self.hass = hass
        self.node_id = node_id
        self.coordinator = coordinator
        self.leases = lease_manager
        self._wakeup = wakeup
        # kv key -> (digest, payload) waiting to be published
        self._pending: dict[str, tuple[bytes, bytes]] = {}
        # kv key -> digest published while this node leads
        self._published: dict[str, bytes] = {}
        self._too_large: dict[str, bytes] = {}
        self._file_stats: dict[str, tuple[int, int] | None] = {}
        self._attached: dict[str, StorageCollection[Any, Any]] = {}
        # Domains whose file was written before the domain was loaded.
        self._reload_on_load: set[str] = set()
        # Domain -> the ids before and after, of a file that was written but
        # whose reload failed: the file is current, so a record that is
        # applied again finds nothing to write and would never reload it.
        self._reload_pending: dict[str, tuple[set[str] | None, set[str] | None]] = {}
        self._apply_lock = asyncio.Lock()
        self._unsub: list[CALLBACK_TYPE] = []
        self._poll_task: asyncio.Task[None] | None = None
        self._watch_task: asyncio.Task[None] | None = None

    @property
    def pending(self) -> bool:
        """Return True if something is waiting to be published."""
        return bool(self._pending)

    async def async_start(self) -> None:
        """Listen to helper changes and start checking the files."""
        self._unsub = [
            self.hass.bus.async_listen(
                EVENT_STATE_CHANGED,
                self._async_on_state_changed,
                event_filter=_is_helper_state,
            ),
            self.hass.bus.async_listen(
                EVENT_COMPONENT_LOADED, self._async_on_component_loaded
            ),
        ]
        for domain in UI_HELPER_DOMAINS:
            if (
                collection := async_get_storage_collection(self.hass, domain)
            ) is not None:
                self._attach(domain, collection)
        self._poll_task = self.hass.async_create_background_task(
            self._async_poll_files(), f"cluster {self.node_id} UI file check"
        )

    async def async_stop(self) -> None:
        """Stop listening and stop the background tasks."""
        for unsub in self._unsub:
            unsub()
        self._unsub = []
        self._attached.clear()
        for task in (self._poll_task, self._watch_task):
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
        self._poll_task = self._watch_task = None

    @callback
    def async_restart_watcher(self) -> None:
        """(Re)start the watcher of the definitions, e.g. after a bucket recreation."""
        if self._watch_task is not None:
            self._watch_task.cancel()
        self._watch_task = self.hass.async_create_background_task(
            self._async_watch(), f"cluster {self.node_id} UI config watcher"
        )

    @callback
    def async_forget_published(self) -> None:
        """Publish everything again (new leadership, or a recreated bucket)."""
        self._published.clear()
        self._too_large.clear()
        self._file_stats.clear()

    @callback
    def _attach(self, domain: str, collection: StorageCollection[Any, Any]) -> None:
        if self._attached.get(domain) is collection:
            return
        self._attached[domain] = collection

        async def _async_changed(change_set: Iterable[CollectionChange]) -> None:
            if self.leases.is_leader:
                self._queue_collection(domain, collection)

        self._unsub.append(collection.async_add_change_set_listener(_async_changed))

    @callback
    def _queue(self, key: str, digest: bytes, payload: dict[str, Any]) -> None:
        if not _valid_kv_key(key):
            return
        self._pending[key] = (
            digest,
            json_bytes({"origin": self.node_id, "ts": time.time(), **payload}),
        )
        self._wakeup()

    @callback
    def _queue_collection(
        self, domain: str, collection: StorageCollection[Any, Any]
    ) -> None:
        payload = {
            "version": collection.store.version,
            "minor_version": collection.store.minor_version,
            "data": collection.async_dump(),
        }
        self._queue(f"{UI_COLLECTION_KEY_PREFIX}{domain}", _digest(payload), payload)

    @callback
    def _queue_state(self, state: State) -> None:
        if (found := _helper_entity(self.hass, state.entity_id)) is None:
            return
        domain, item_id, entity = found
        try:
            extra_data = entity.extra_restore_state_data
        except Exception:
            _LOGGER.exception("Could not get the restore data of %s", state.entity_id)
            return
        stored = StoredState(state, extra_data, dt_util.utcnow()).as_dict()
        self._queue(
            f"{UI_STATE_KEY_PREFIX}{domain}.{item_id}",
            _digest([stored["state"], stored["extra_data"]]),
            {"stored": stored},
        )

    @callback
    def _async_on_state_changed(self, event: Event[EventStateChangedData]) -> None:
        if self.leases.is_leader and (state := event.data["new_state"]) is not None:
            self._queue_state(state)

    async def _async_check_files(self, *, force: bool) -> None:
        """Queue each UI file that changed (``force``: every file)."""
        for name, domain in UI_CONFIG_FILES.items():
            path = self.hass.config.path(name)
            stat = await self.hass.async_add_executor_job(_stat, path)
            if stat is None:
                if self._file_stats.get(name) is not None:
                    # It was there and is gone: tell the standbys, or they
                    # keep (and at a takeover apply) the old content.
                    self._file_stats[name] = None
                    payload = {"domain": domain, "content": None, "deleted": True}
                    self._queue(
                        f"{UI_FILE_KEY_PREFIX}{name}", _digest(payload), payload
                    )
                continue
            if not force and stat == self._file_stats.get(name):
                continue
            self._file_stats[name] = stat
            if (
                content := await self.hass.async_add_executor_job(_read_text, path)
            ) is None:
                continue
            payload = {"domain": domain, "content": content}
            self._queue(f"{UI_FILE_KEY_PREFIX}{name}", _digest(payload), payload)

    async def _async_poll_files(self) -> None:
        while True:
            await asyncio.sleep(_POLL_INTERVAL)
            await self._async_retry_reloads()
            if not self.leases.is_leader:
                continue
            try:
                await self._async_check_files(force=False)
            except Exception:
                _LOGGER.exception("Checking the UI configuration files failed")

    async def async_queue_all(self) -> None:
        """Queue every file, collection and helper state (this node leads)."""
        await self._async_check_files(force=True)
        for domain in UI_HELPER_DOMAINS:
            if (collection := async_get_storage_collection(self.hass, domain)) is None:
                continue
            self._attach(domain, collection)
            self._queue_collection(domain, collection)
            if (
                component := self.hass.data.get(DATA_INSTANCES, {}).get(domain)
            ) is None:
                continue
            for entity in list(component.entities):
                if (state := self.hass.states.get(entity.entity_id)) is not None:
                    self._queue_state(state)

    async def async_drain(self) -> bool:
        """Attempt every queued record once; return False if one must be retried."""
        batch, self._pending = self._pending, {}
        ok = True
        for key, (digest, payload) in batch.items():
            try:
                published = await self._async_publish(key, digest, payload)
            except Exception:
                _LOGGER.exception("Publishing %s failed", key)
                published = False
            if not published:
                # A newer value of the same record wins.
                self._pending.setdefault(key, (digest, payload))
                ok = False
        return ok

    async def _async_publish(self, key: str, digest: bytes, payload: bytes) -> bool:
        if not self.leases.is_leader or self._published.get(key) == digest:
            return True
        if len(payload) > self.coordinator.max_payload:
            if self._too_large.get(key) == digest:
                return True
            _LOGGER.warning(
                "%s (%d bytes) exceeds the NATS max_payload of %d bytes and is"
                " not synced to the other nodes; raise max_payload on the server",
                key,
                len(payload),
                self.coordinator.max_payload,
            )
            if not self.coordinator.connected:
                return False
            # An earlier, smaller version must not be taken for this one: the
            # other nodes keep their own copy instead.
            try:
                await self.coordinator.async_fenced_delete(key)
            except TermSuperseded:
                return True  # no longer this node's to write
            except Exception as err:  # noqa: BLE001 - the caller queues it again
                self.coordinator.note_error(err)
                return False
            self._too_large[key] = digest
            self._published.pop(key, None)
            return True
        if not self.coordinator.connected:
            return False
        term = self.leases.lease_term
        try:
            await self.coordinator.async_fenced_put(key, payload)
        except TermSuperseded:
            return True  # no longer this node's to write
        except Exception as err:  # noqa: BLE001 - the caller queues it again
            self.coordinator.note_error(err)
            _LOGGER.warning("Could not publish %s: %s", key, err)
            return False
        if self.leases.is_leader and self.leases.lease_term == term:
            self._published[key] = digest
        return True

    async def _async_watch(self) -> None:
        """Apply the leader's definitions while this node is a standby."""
        while True:
            try:
                kv = self.coordinator.kv_storage
                if TYPE_CHECKING:
                    assert kv is not None
                # Replays every current record when it starts.
                watcher = await kv.watch(f"{UI_KEY_PREFIX}>")
                try:
                    async for update in watcher:
                        if update is None or update.operation in ("DEL", "PURGE"):
                            continue
                        if LEADER_KEY in self.leases.held:
                            continue  # leads, or is being promoted
                        await self._async_apply_raw(update.key, update.value)
                finally:
                    await stop_watcher(watcher)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                self.coordinator.note_error(err)
                _LOGGER.debug("UI config watcher restarting: %s", err)
            await asyncio.sleep(1)

    async def _async_apply_raw(
        self, key: str, value: bytes | None, *, strict: bool = False
    ) -> None:
        """Apply a record.

        ``strict`` (a promotion): only a record with wrong content is skipped;
        any other failure raises ``HandoverIncomplete``.
        """
        try:
            payload = json_loads_object(value or b"")
        except ValueError:
            _LOGGER.warning("Ignoring the malformed record %s", key)
            return
        async with self._apply_lock:
            try:
                await self._async_apply(key, payload)
            except DATA_ERRORS as err:
                _LOGGER.error("Not applying %s: its content is invalid: %r", key, err)
            except Exception as err:
                if strict:
                    raise HandoverIncomplete(f"could not apply {key}: {err!r}") from err
                _LOGGER.warning("Could not apply %s: %r", key, err)

    async def _async_apply(self, key: str, payload: dict[str, Any]) -> None:
        """Decode a record, then apply it.

        What is wrong with the record raises one of ``DATA_ERRORS`` before
        anything changed. Whatever fails while applying it may have changed
        something already: that raises ``ApplyFailed``, never a data error.
        """
        action = self._decode(key, payload)
        if action is None:
            return
        try:
            await action()
        except Exception as err:
            raise ApplyFailed(f"{key}: {err!r}") from err

    def _decode(
        self, key: str, payload: dict[str, Any]
    ) -> Callable[[], Awaitable[None]] | None:
        """Check a record and return what applies it (None: nothing to do)."""
        if payload.get("origin") == self.node_id:
            return None
        if key.startswith(UI_FILE_KEY_PREFIX):
            name = key.removeprefix(UI_FILE_KEY_PREFIX)
            # Never write a file whose name came from NATS unless it is one of
            # the three the config editor writes.
            if name not in UI_CONFIG_FILES:
                return None
            content = payload["content"]
            if payload.get("deleted") is True:
                content = None
            elif not isinstance(content, str):
                raise TypeError("the content is not text")
            return lambda: self._async_apply_file(name, content)
        if key.startswith(UI_COLLECTION_KEY_PREFIX):
            domain = key.removeprefix(UI_COLLECTION_KEY_PREFIX)
            if domain not in UI_HELPER_DOMAINS:
                return None
            for field in ("version", "minor_version", "data"):
                payload[field]
            return lambda: self._async_apply_collection(domain, payload)
        return None

    async def _async_apply_file(self, name: str, content: str | None) -> None:
        domain = UI_CONFIG_FILES[name]
        path = self.hass.config.path(name)
        ids = await self.hass.async_add_executor_job(
            _replace_file, path, domain, content
        )
        if ids is not None:
            old_ids, new_ids = ids
            if (earlier := self._reload_pending.get(domain)) is not None:
                old_ids = earlier[0]  # the state before the first unreloaded write
            self._reload_pending[domain] = (old_ids, new_ids)
            _LOGGER.info("Node %s wrote the leader's %s", self.node_id, name)
        elif domain not in self._reload_pending:
            return
        # Else the file is current, but an earlier reload of it failed.
        await self._async_reload(domain)

    async def _async_retry_reloads(self) -> None:
        """Reload the files that were written, and whose reload failed."""
        for domain in list(self._reload_pending):
            async with self._apply_lock:
                if domain not in self._reload_pending:
                    continue
                try:
                    await self._async_reload(domain)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.warning("Reloading %s failed again: %r", domain, err)

    async def _async_reload(self, domain: str) -> None:
        """Reload ``domain`` after its file was written; raise if that fails.

        The failure leaves the domain pending: nothing is forgotten until the
        reload went through.
        """
        old_ids, new_ids = self._reload_pending[domain]
        if self.hass.services.has_service(domain, SERVICE_RELOAD):
            # Not through the router: a standby would forward the call.
            await self.hass.services._async_call_local(  # noqa: SLF001
                domain, SERVICE_RELOAD, blocking=True
            )
        else:
            self._reload_on_load.add(domain)
        del self._reload_pending[domain]
        if old_ids is None or new_ids is None:
            return
        # Deleted in the editor: it removes the registry entry, as here.
        ent_reg = er.async_get(self.hass)
        for item_id in old_ids - new_ids:
            if (
                entity_id := ent_reg.async_get_entity_id(
                    domain, _REGISTRY_PLATFORMS[domain], item_id
                )
            ) is not None:
                ent_reg.async_remove(entity_id)

    async def _async_apply_collection(
        self, domain: str, payload: dict[str, Any]
    ) -> None:
        if (collection := async_get_storage_collection(self.hass, domain)) is None:
            return  # applied when the integration is loaded
        version = (payload["version"], payload["minor_version"])
        if version != (collection.store.version, collection.store.minor_version):
            _LOGGER.warning(
                "Not applying the leader's %s helpers: their storage version is"
                " %s.%s, this node's %s.%s",
                domain,
                *version,
                collection.store.version,
                collection.store.minor_version,
            )
            return
        await collection.async_replace(payload["data"])

    async def _async_on_component_loaded(self, event: Event) -> None:
        domain = event.data.get("component")
        if domain in UI_HELPER_DOMAINS:
            if (collection := async_get_storage_collection(self.hass, domain)) is None:
                return
            self._attach(domain, collection)
            if self.leases.is_leader:
                self._queue_collection(domain, collection)
            elif LEADER_KEY not in self.leases.held:
                await self._async_fetch(f"{UI_COLLECTION_KEY_PREFIX}{domain}")
        elif domain in self._reload_on_load:
            # Bootstrap read the file before this node wrote the leader's.
            self._reload_on_load.discard(domain)
            if self.hass.services.has_service(domain, SERVICE_RELOAD):
                await self.hass.services._async_call_local(  # noqa: SLF001
                    domain, SERVICE_RELOAD, blocking=True
                )

    async def _async_fetch(self, key: str) -> None:
        if (
            kv := self.coordinator.kv_storage
        ) is None or not self.coordinator.connected:
            return
        try:
            current = await kv.get(key)
        except KeyNotFoundError, KeyDeletedError:
            return
        except Exception as err:  # noqa: BLE001 - the watcher delivers later changes
            _LOGGER.debug("Could not read %s: %s", key, err)
            return
        await self._async_apply_raw(key, current.value)

    # Promotion

    async def async_before_promote(self) -> None:
        """Apply every definition, then take the helper states over.

        Raises ``HandoverIncomplete`` if a record cannot be read.
        """
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            raise HandoverIncomplete("NATS is not connected")
        try:
            keys = await kv.keys()
        except NoKeysError:
            return
        except Exception as err:
            raise HandoverIncomplete(f"could not list the records: {err!r}") from err
        states: dict[str, bytes] = {}
        for key in sorted(keys):
            if not key.startswith((UI_KEY_PREFIX, UI_STATE_KEY_PREFIX)):
                continue
            try:
                current = await kv.get(key)
            except KeyNotFoundError, KeyDeletedError:
                continue
            except Exception as err:
                raise HandoverIncomplete(f"could not read {key}: {err!r}") from err
            if key.startswith(UI_STATE_KEY_PREFIX):
                states[key] = current.value or b""
            else:
                await self._async_apply_raw(key, current.value, strict=True)
        for key, value in states.items():
            try:
                await self._async_take_state(key, value)
            except Exception as err:  # noqa: BLE001 - one bad record must not stop the rest
                _LOGGER.warning("Could not take over %s: %r", key, err)

    async def _async_take_state(self, key: str, value: bytes) -> None:
        """Create a helper entity again so that it restores the leader's state."""
        domain, _, item_id = key.removeprefix(UI_STATE_KEY_PREFIX).partition(".")
        if domain not in UI_HELPER_DOMAINS:
            return
        collection = async_get_storage_collection(self.hass, domain)
        component = self.hass.data.get(DATA_INSTANCES, {}).get(domain)
        if collection is None or component is None or item_id not in collection.data:
            return
        entity_id = er.async_get(self.hass).async_get_entity_id(domain, domain, item_id)
        entity = component.get_entity(entity_id) if entity_id else None
        if not isinstance(entity, CollectionEntity) or not isinstance(
            entity, RestoreEntity
        ):
            return
        stored = StoredState.from_dict(json_loads_object(value)["stored"])  # type: ignore[arg-type]
        current = self.hass.states.get(entity.entity_id)
        if (
            current is not None
            and current.state == stored.state.state
            and current.attributes == stored.state.attributes
        ):
            return
        registry_id = entity.registry_entry.id if entity.registry_entry else None
        await entity.async_remove()
        data = restore_state.async_get(self.hass)
        stored.entity_registry_id = registry_id
        if registry_id is not None:
            data.last_states_by_entity_registry_id[registry_id] = stored
        else:
            data.last_states_by_entity_id[entity.entity_id] = stored
        await component.async_add_entities(
            [collection.create_entity(type(entity), collection.data[item_id])]
        )
        _LOGGER.info(
            "Node %s took over the state of %s from the leader",
            self.node_id,
            entity.entity_id,
        )


@callback
def _is_helper_state(event_data: EventStateChangedData) -> bool:
    return event_data["entity_id"].partition(".")[0] in UI_HELPER_DOMAINS


def _helper_entity(
    hass: HomeAssistant, entity_id: str
) -> tuple[str, str, RestoreEntity] | None:
    """The domain, item id and entity of a live helper made in the UI."""
    domain = entity_id.partition(".")[0]
    if domain not in UI_HELPER_DOMAINS:
        return None
    collection = async_get_storage_collection(hass, domain)
    component = hass.data.get(DATA_INSTANCES, {}).get(domain)
    if collection is None or component is None:
        return None
    entity = component.get_entity(entity_id)
    if (
        not isinstance(entity, CollectionEntity)
        or not isinstance(entity, RestoreEntity)
        or (item_id := entity.unique_id) is None
        or item_id not in collection.data
    ):
        return None
    return domain, item_id, entity
