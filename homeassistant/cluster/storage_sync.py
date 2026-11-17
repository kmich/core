"""Mirror ``.storage/`` writes into the disk-backed ``hass_storage`` bucket.

Fed by Core's store write listener (``storage.async_track_store_writes``).
Two kinds of keys are written:

``store.<key>``
    The exact content HA just wrote for a store, wrapped with origin and
    timestamp. A durable, versioned (history=3) off-box copy of every
    registry, useful to seed or rebuild a node. It is **not** applied to the
    other nodes' disks while they run: their registries live in memory and
    would overwrite the file on their next save.

``entry.<entry_id>``
    One config entry in its storage form (``ConfigEntry.as_dict``: data,
    options, version, unique id, title, disabled state, subentries, ...),
    published only by the leader, for every entry of a clustered domain and
    in any state, as soon as the entry changes (not after the delayed save of
    the config entries store). A removed entry leaves a tombstone under the
    same key (``{"removed": true, ...}``); a missing key still means "never
    published".

    Standbys apply these records: live, through a watcher (an entry the
    leader added appears, an entry it removed goes without the integration's
    clean-up, a disabled entry follows, subentries follow), and in full when
    a node is promoted, *before* it sets entries up. This is what makes
    OAuth2 failover work: a refresh token rotated on node A is the one node B
    starts with, instead of the stale copy in B's memory. The node that kept
    holding the lease never applies: its own copy is the newest (a reload
    right after an options change must not adopt the record from before the
    change).

Automations, scripts, scenes and helpers made in the leader's UI are
published and applied by ``ui_config_sync``, through this module's writer.

At shutdown the integration calls ``async_flush`` before it releases the
leases, so changes written by the final save are published first.
"""

import asyncio
from collections.abc import Mapping
from contextlib import suppress
from datetime import datetime
import hashlib
import logging
import time
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, cast

from nats.js.errors import KeyDeletedError, KeyNotFoundError, NoKeysError

from homeassistant.config_entries import (
    SIGNAL_CONFIG_ENTRY_CHANGED,
    ConfigEntry,
    ConfigEntryChange,
    ConfigEntryDisabler,
    ConfigSubentry,
    OperationNotAllowed,
)
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import cluster as cluster_helper
from homeassistant.helpers.discovery_flow import DiscoveryKey
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.json import json_bytes
from homeassistant.helpers.restore_state import DATA_RESTORE_STATE
from homeassistant.helpers.storage import STORAGE_DIR, async_track_store_writes
from homeassistant.util.enum import try_parse_enum
from homeassistant.util.json import json_loads

from .const import (
    CONFIG_ENTRIES_STORE,
    ENTRY_KEY_PREFIX,
    LEADER_KEY,
    MIRRORED_STORES,
    PROTOCOL_VERSION,
    STORAGE_KEY_PREFIX,
)
from .coordinator import ClusterCoordinator, TermSuperseded, stop_watcher
from .crypto import RecordCipher, RecordKeyError
from .durability import (
    NodeMemory,
    TakeoverWatch,
    async_check_credentials,
    async_clear_data_lost,
    async_raise_data_lost,
    build_inventory,
    parse_inventory,
)
from .lease_manager import DATA_ERRORS, HandoverIncomplete, LeaseManager
from .protocol import UnsupportedProtocol, check_record
from .registry_sync import async_load_registries
from .ui_config_sync import UiConfigSync

_LOGGER = logging.getLogger(__name__)

# Seconds between two attempts to publish what failed (doubling).
_RETRY_MIN = 1.0
_RETRY_MAX = 30.0
# ``async_flush`` makes this many passes, this far apart, before it gives up.
_FLUSH_PASSES = 3
_FLUSH_PASS_INTERVAL = 0.5

_ENTRY_FIELDS = (
    "data",
    "options",
    "title",
    "unique_id",
    "version",
    "minor_version",
    "disabled_by",
    "pref_disable_new_entities",
    "pref_disable_polling",
)
_ENVELOPE_OVERHEAD = 256

# The record of the entries a leader runs and the latest removals.
INVENTORY_KEY = "inventory"


def _validate_record(payload: Any) -> None:
    """Raise one of ``DATA_ERRORS`` if a record cannot be applied at all.

    Runs before anything is changed; what it lets through and fails later is a
    failure to apply, not a wrong record.
    """
    if not isinstance(payload, dict):
        raise TypeError("the record is not an object")
    if payload.get("removing") or payload.get("oversized"):
        return
    if payload.get("removed"):
        if not isinstance(payload["entry_id"], str):
            raise TypeError("the entry id is not text")
        return
    record = payload["entry"]
    if not isinstance(record, dict):
        raise TypeError("the entry is not an object")
    for field in ("entry_id", "domain", "created_at"):
        if not isinstance(record[field], str):
            raise TypeError(f"{field} is not text")
    datetime.fromisoformat(record["created_at"])
    for field in ("data", "options"):
        if not isinstance(record[field], Mapping):
            raise TypeError(f"{field} is not an object")


def _oversized_message(kv_key: str) -> str:
    return (
        f"the record {kv_key} is out of date: the entry grew past the NATS"
        " max_payload and the leader could not replicate it. Raise max_payload"
        " on the NATS server"
    )


def _record_digest(record: dict[str, Any]) -> bytes:
    """Hash of an entry record without its modification time.

    Applying a record sets ``modified_at`` to now, so the time must not take
    part: an applied record must hash like the entry it produced.
    """
    return hashlib.sha256(
        json_bytes({k: v for k, v in record.items() if k != "modified_at"})
    ).digest()


def _valid_kv_key(key: str) -> bool:
    return bool(key) and all(c.isalnum() or c in "-/_=." for c in key)


class StorageSync:
    """Mirrors store writes and hands config entry data over on failover."""

    def __init__(
        self,
        hass: HomeAssistant,
        node_id: str,
        coordinator: ClusterCoordinator,
        lease_manager: LeaseManager,
        *,
        exclude: set[str] | frozenset[str],
        cipher: RecordCipher | None = None,
    ) -> None:
        """Initialize storage sync.

        ``cipher``: encrypts the config entry records; None publishes them as
        they are (``security: permissive`` only).
        """
        self._cipher = cipher
        # Records this node cannot read: logged once per key.
        self._unreadable: set[str] = set()
        self.hass = hass
        self.node_id = node_id
        self.coordinator = coordinator
        self.leases = lease_manager
        self.exclude = frozenset(exclude)
        self._last_hash: dict[str, bytes] = {}
        self._too_large: set[str] = set()
        # store key -> latest content written; drained by a single writer so
        # that two writes of one store can never reach the bucket reordered
        self._pending: dict[str, str | bytes] = {}
        # entry ids whose record must be published (their current content)
        self._pending_entries: set[str] = set()
        # entry id -> domain of removed entries whose tombstone must be published
        self._pending_tombstones: dict[str, str] = {}
        # The inventory must be published again (entries added or removed).
        self._inventory_dirty = False
        # Entries whose removal was announced (``removing``) and not finished.
        self._removing: set[str] = set()
        # kv key -> digest of an entry record that is too large to publish
        self._entry_too_large: dict[str, bytes] = {}
        # Removals a takeover completes; the placement lets them through.
        self.completing_removals: set[str] = set()
        # True while a record of the leader is applied (the placement lets
        # that change through).
        self.applying_record = False
        # Their tombstones, published once this node leads.
        self._deferred_tombstones: dict[str, str] = {}
        # entry id -> when it changed, for the publication latency
        self._changed_at: dict[str, float] = {}
        self.memory = NodeMemory(hass)
        self.takeover = TakeoverWatch(hass, lambda e: self._replicates(e.domain))
        self._wakeup = asyncio.Event()
        self._writer = asyncio.Lock()
        # Applying records must not interleave with itself.
        self._apply_lock = asyncio.Lock()
        self._unsub: list[CALLBACK_TYPE] = []
        self._task: asyncio.Task[None] | None = None
        self._watch_task: asyncio.Task[None] | None = None
        self.ui = UiConfigSync(
            hass, node_id, coordinator, lease_manager, wakeup=self._wakeup.set
        )

    async def async_start(self) -> None:
        """Listen to store writes and entry changes; register the pull."""
        await self.memory.async_load()
        if self._cipher is None:
            _LOGGER.warning(
                "Config entry records (tokens, API keys) are published to NATS"
                " without encryption: set encryption_key on every node"
            )
        self._unsub = [
            async_track_store_writes(
                self.hass,
                self._async_on_write,
                MIRRORED_STORES | {CONFIG_ENTRIES_STORE},
            ),
            async_dispatcher_connect(
                self.hass, SIGNAL_CONFIG_ENTRY_CHANGED, self._async_on_entry_changed
            ),
        ]
        self._task = self.hass.async_create_background_task(
            self._async_flush_loop(), f"cluster {self.node_id} storage mirror"
        )
        await self.ui.async_start()
        self.async_restart_watcher()
        self.leases.add_before_setup(self._async_pull_entry)
        self.leases.add_before_setup(self._async_check_credentials)
        # Entries we already hold may have been written before we started.
        await self._async_publish_entries()

    async def async_stop(self) -> None:
        """Stop listening."""
        for unsub in self._unsub:
            unsub()
        self._unsub = []
        self.takeover.async_stop()
        await self.ui.async_stop()
        for task in (self._task, self._watch_task):
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
        self._task = self._watch_task = None

    @callback
    def async_restart_watcher(self) -> None:
        """(Re)start the watchers of the records, e.g. after a bucket recreation."""
        if self._watch_task is not None:
            self._watch_task.cancel()
        self._watch_task = self.hass.async_create_background_task(
            self._async_watch_entries(), f"cluster {self.node_id} entry watcher"
        )
        self.ui.async_restart_watcher()

    def _replicates(self, domain: str) -> bool:
        """Return True if config entries of ``domain`` are replicated.

        Every entry of a clustered domain is, in any state: also disabled and
        ignored ones, which run nowhere.
        """
        return self.leases.domain_is_clustered(domain)

    async def async_flush(self) -> None:
        """Publish everything queued, e.g. before the leases are released."""
        self._pending_entries.update(
            entry.entry_id
            for entry in self.hass.config_entries.async_entries()
            if self.leases.is_leader
            and self._replicates(entry.domain)
            and not self.leases.is_handover(entry.entry_id)
        )
        if self.leases.is_leader:
            # A store that did not change is not written by Core, so it would
            # not be in the bucket when the next leader needs it.
            await self._async_queue_stored_files(only_missing=True)
            await self.ui.async_queue_all()
            self._inventory_dirty = True
            self._pending_tombstones.update(self._deferred_tombstones)
            self._deferred_tombstones.clear()
        for attempt in range(_FLUSH_PASSES):
            if attempt:
                await asyncio.sleep(_FLUSH_PASS_INTERVAL)
            await self._async_drain()
            if (
                not self._pending
                and not self._pending_entries
                and not self._pending_tombstones
                and not self._inventory_dirty
                and not self.ui.pending
            ):
                return
        raise HomeAssistantError("Could not flush all storage writes to JetStream")

    def _seal(self, kv_key: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Return ``payload`` with its entry encrypted (if a key is configured)."""
        if self._cipher is None or "entry" not in payload:
            return payload
        sealed = {k: v for k, v in payload.items() if k != "entry"}
        sealed["enc"] = self._cipher.seal(kv_key, payload["entry"])
        return sealed

    def _open(self, kv_key: str, payload: Any) -> Any:
        """Return ``payload`` with its entry decrypted.

        Raises ``RecordKeyError`` for a record this node cannot read: another
        key, damage, an encrypted record without a key here, or (the downgrade)
        a plain entry while a key is configured.
        """
        if not isinstance(payload, dict):
            return payload
        if "enc" in payload:
            if self._cipher is None:
                raise RecordKeyError(
                    f"{kv_key} is encrypted, but this node has no encryption_key"
                )
            opened = {k: v for k, v in payload.items() if k != "enc"}
            opened["entry"] = self._cipher.open(kv_key, payload["enc"])
            return opened
        if self._cipher is not None and "entry" in payload:
            raise RecordKeyError(
                f"{kv_key} holds a config entry in plain text while this node"
                " encrypts them: refused"
            )
        return payload

    async def _async_queue_stored_files(self, *, only_missing: bool = False) -> None:
        """Queue the saved copy of every mirrored store that is not queued.

        A store that did not change since the bucket was lost is not written
        again by Home Assistant, so the saved file is the copy to publish.
        ``only_missing``: only for stores the bucket has no record of. After a
        takeover the saved file is this node's copy from before, and the
        record in the bucket is the previous leader's newer one.
        """
        kv = self.coordinator.kv_storage
        for key in MIRRORED_STORES - self.exclude:
            if key in self._pending:
                continue
            if only_missing and kv is not None:
                try:
                    current = await kv.get(f"{STORAGE_KEY_PREFIX}{key}")
                except KeyNotFoundError, KeyDeletedError:
                    pass
                except Exception as err:  # noqa: BLE001 - the next flush tries again
                    self.coordinator.note_error(err)
                    continue
                else:
                    if current.value:
                        continue
            if (
                content := await self.hass.async_add_executor_job(
                    _read_file, self.hass.config.path(STORAGE_DIR, key)
                )
            ) is not None:
                self._pending.setdefault(key, content)

    async def _async_republish_intents(self) -> None:
        """Announce the removals that are not complete again, before the inventory."""
        if not self.leases.is_leader:
            return
        for entry_id, payload in list(self.memory.intents.items()):
            try:
                await self.coordinator.async_fenced_put(
                    f"{ENTRY_KEY_PREFIX}{entry_id}",
                    json_bytes(
                        self._seal(
                            f"{ENTRY_KEY_PREFIX}{entry_id}",
                            {
                                **payload,
                                "protocol": PROTOCOL_VERSION,
                                "term": self.coordinator.term,
                            },
                        )
                    ),
                )
            except Exception as err:  # noqa: BLE001 - the next republish retries
                self.coordinator.note_error(err)
                _LOGGER.warning(
                    "Could not announce the removal of %s: %s", entry_id, err
                )
                continue
            self._removing.add(entry_id)

    async def async_republish(self) -> None:
        """Publish every held entry again (the bucket was recreated)."""
        self._last_hash.clear()
        self._too_large.clear()
        self.ui.async_forget_published()
        await self._async_queue_stored_files()
        await self._async_republish_intents()
        try:
            await self.async_flush()
        except HomeAssistantError as err:
            # The background loop keeps retrying what is still queued.
            _LOGGER.warning("Could not republish after the bucket check: %s", err)

    @callback
    def _async_on_write(self, key: str, content: str | bytes) -> None:
        if key != CONFIG_ENTRIES_STORE and key not in MIRRORED_STORES:
            return
        self._pending[key] = content
        self._wakeup.set()

    @callback
    def _async_on_entry_changed(
        self, change: ConfigEntryChange, entry: ConfigEntry
    ) -> None:
        if not (self.leases.is_leader and self._replicates(entry.domain)):
            return
        if change is ConfigEntryChange.REMOVED:
            self._pending_entries.discard(entry.entry_id)
            if self._cleanup_failed(entry.entry_id):
                # No tombstone: the announcement stays, and the node that
                # takes over runs the clean-up again.
                _LOGGER.error(
                    "The clean-up of the removed %s (%s) failed; the next leader"
                    " repeats it",
                    entry.title,
                    entry.domain,
                )
                self._removing.add(entry.entry_id)
                return
            self._changed_at.pop(entry.entry_id, None)
            self._last_hash.pop(f"{ENTRY_KEY_PREFIX}{entry.entry_id}", None)
            self._pending_tombstones[entry.entry_id] = entry.domain
            self.memory.async_note_removed(entry.entry_id)
            self._inventory_dirty = True
        else:
            if change is ConfigEntryChange.ADDED:
                self._inventory_dirty = True
            self._pending_tombstones.pop(entry.entry_id, None)
            self._pending_entries.add(entry.entry_id)
            self._changed_at.setdefault(entry.entry_id, time.monotonic())
        self._wakeup.set()

    async def async_publish_removing(self, entry: ConfigEntry) -> None:
        """Announce a removal before its clean-up runs (on the leader).

        If this node stops before the removal finished, the node that takes
        over finds the announcement and completes it, clean-up included.
        Raises ``OperationNotAllowed`` if the announcement fails.
        """
        if not self.leases.is_leader or not self._replicates(entry.domain):
            return
        payload = {
            "protocol": PROTOCOL_VERSION,
            "origin": self.node_id,
            "term": self.coordinator.term,
            "ts": time.time(),
            "removing": True,
            "entry_id": entry.entry_id,
            "domain": entry.domain,
            # What a node without a copy needs to run the integration's
            # clean-up.
            "entry": entry.as_dict(),
        }
        async with self._writer:
            try:
                kv_key = f"{ENTRY_KEY_PREFIX}{entry.entry_id}"
                await self.coordinator.async_fenced_put(
                    kv_key, json_bytes(self._seal(kv_key, payload))
                )
                await self.memory.async_note_intent(entry.entry_id, payload)
            except Exception as err:
                self.coordinator.note_error(err)
                raise OperationNotAllowed(
                    "Could not publish the removal to the cluster; try again"
                ) from err
        self._removing.add(entry.entry_id)
        self._pending_entries.discard(entry.entry_id)
        self._last_hash.pop(f"{ENTRY_KEY_PREFIX}{entry.entry_id}", None)

    async def _async_check_credentials(self, entry: ConfigEntry) -> None:
        if self._replicates(entry.domain) and self.leases.is_handover(entry.entry_id):
            await async_check_credentials(self.hass, entry)

    async def _async_publish_inventory(self) -> bool:
        """Publish the inventory; return False if it must be retried."""
        if not self.leases.is_leader:
            return True
        record = build_inventory(
            self.node_id,
            self.coordinator.term,
            (
                entry.entry_id
                for entry in self.hass.config_entries.async_entries()
                if self._replicates(entry.domain)
                and entry.entry_id not in self._removing
            ),
            self.memory,
        )
        try:
            await self.coordinator.async_fenced_put(INVENTORY_KEY, json_bytes(record))
        except TermSuperseded:
            return True
        except Exception as err:  # noqa: BLE001 - the caller queues it again
            self.coordinator.note_error(err)
            _LOGGER.warning("Could not publish the inventory: %s", err)
            return False
        self.memory.async_mark_seen_inventory()
        return True

    async def _async_flush_loop(self) -> None:
        delay = _RETRY_MIN
        while True:
            await self._wakeup.wait()
            self._wakeup.clear()
            if await self._async_drain():
                delay = _RETRY_MIN
                continue
            # Something could not be published: it is queued again. Retry
            # slowly instead of hammering a server that is not answering.
            await asyncio.sleep(delay)
            delay = min(delay * 2, _RETRY_MAX)
            self._wakeup.set()

    async def _async_drain(self) -> bool:
        """Attempt every queued item once; return False if any must be retried."""
        async with self._writer:
            entry_ids, self._pending_entries = self._pending_entries, set()
            tombstones, self._pending_tombstones = self._pending_tombstones, {}
            batch, self._pending = self._pending, {}
            ok = True
            for entry_id in entry_ids:
                if (
                    entry := self.hass.config_entries.async_get_entry(entry_id)
                ) is None or entry_id in self._removing:
                    continue
                try:
                    published = await self._async_publish_entry(entry)
                except Exception:
                    _LOGGER.exception("Publishing entry %s failed", entry_id)
                    published = False
                if not published:
                    self._pending_entries.add(entry_id)
                    ok = False
            for entry_id, domain in tombstones.items():
                if self.hass.config_entries.async_get_entry(entry_id) is not None:
                    continue  # added again: its record replaces the tombstone
                if not self.leases.is_leader and self._replicates(domain):
                    # Only a leader can confirm the removal. Not done: the
                    # announcement stays, and the intent with it, until a
                    # tombstone was published in a term of this node (or the
                    # next leader completed the removal).
                    self._deferred_tombstones[entry_id] = domain
                    continue
                try:
                    published = await self._async_publish_tombstone(entry_id, domain)
                except Exception:
                    _LOGGER.exception("Publishing the removal of %s failed", entry_id)
                    published = False
                if not published:
                    self._pending_tombstones.setdefault(entry_id, domain)
                    ok = False
                else:
                    self._removing.discard(entry_id)
                    self.memory.async_clear_intent(entry_id)
            if self._inventory_dirty:
                self._inventory_dirty = False
                if not await self._async_publish_inventory():
                    self._inventory_dirty = True
                    ok = False
            if not await self.ui.async_drain():
                ok = False
            for key, content in batch.items():
                try:
                    mirrored = await self.async_mirror(key, content)
                except Exception:
                    _LOGGER.exception("Mirroring store %s failed", key)
                    mirrored = False
                if not mirrored:
                    # A newer write of the same store wins.
                    self._pending.setdefault(key, content)
                    ok = False
            return ok

    async def async_before_promote(self, is_handover: bool) -> None:
        """Start a term, then pull handover data before promotion."""
        # Before anything is read: once the term key moved, a former leader
        # that is still running can no longer write what this node reads.
        try:
            await self.coordinator.async_start_term(self.node_id)
        except Exception as err:
            raise HandoverIncomplete(f"could not start a term: {err}") from err
        _LOGGER.info("Node %s started term %s", self.node_id, self.coordinator.term)
        # A new leadership: publish the UI configuration again.
        self.ui.async_forget_published()
        if not is_handover:
            return
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            raise HandoverIncomplete("NATS is not connected")
        try:
            current = await kv.get(f"{STORAGE_KEY_PREFIX}core.restore_state")
        except KeyNotFoundError, KeyDeletedError:
            current = None  # never published
        except Exception as err:
            # A timeout is not "no checkpoint": the previous leader's states
            # would be lost. Another attempt is made after the back-off.
            raise HandoverIncomplete(
                f"could not read the restore_state checkpoint: {err!r}"
            ) from err
        if current is not None:
            try:
                envelope = json_loads(current.value or b"{}")
                if (
                    isinstance(envelope, dict)
                    and envelope.get("origin") != self.node_id
                ):
                    content = envelope.get("content")
                    if isinstance(content, dict) and "data" in content:
                        stored_states = content["data"]
                        restore = self.hass.data.get(DATA_RESTORE_STATE)
                        if restore is not None and isinstance(stored_states, list):
                            restore._async_load_stored_states(  # noqa: SLF001
                                cast(list[dict[str, Any]], stored_states)
                            )
            except Exception as err:  # noqa: BLE001
                # Read, but not usable: it is no better on the next attempt.
                _LOGGER.warning("Could not adopt restore_state on handover: %s", err)

        # Apply every config entry record (entries added, changed or removed
        # while this node was a standby) before placement opens; the entries
        # created here become part of the handover.
        await self._async_apply_all()
        # Then remove what the records no longer show (lost tombstones).
        await self._async_reconcile_inventory()
        self.takeover.async_took_over(self.leases.previous_leader)
        # Then the registries: the entries they belong to exist now.
        await async_load_registries(self.hass, self.node_id, kv, self.leases)
        # Then what was made in the leader's UI, and the helpers' states.
        await self.ui.async_before_promote()

    async def _async_apply_all(self) -> None:
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            raise HandoverIncomplete("NATS is not connected")
        try:
            keys = [key for key in await kv.keys() if key.startswith(ENTRY_KEY_PREFIX)]
        except NoKeysError:
            return  # nothing was ever published
        except Exception as err:
            raise HandoverIncomplete(
                f"could not list the entry records: {err!r}"
            ) from err
        removing: list[tuple[str, str]] = []
        for key in keys:
            try:
                current = await kv.get(key)
            except KeyNotFoundError, KeyDeletedError:
                continue
            except Exception as err:
                # A timeout is not "no record": the previous owner may have
                # rotated credentials that this node's copy does not have.
                raise HandoverIncomplete(
                    f"could not read the entry record {key}: {err!r}"
                ) from err
            try:
                payload = json_loads(current.value or b"")
            except ValueError:
                _LOGGER.warning("Ignoring malformed handover record %s", key)
                continue
            try:
                check_record(key, payload)
                payload = self._open(key, payload)
            except (RecordKeyError, UnsupportedProtocol) as err:
                # Not a record with wrong content that can be skipped: this
                # node would lead with an older copy of the entry.
                raise HandoverIncomplete(str(err)) from err
            if isinstance(payload, dict) and payload.get("oversized"):
                raise HandoverIncomplete(_oversized_message(key))
            if (
                isinstance(payload, dict)
                and payload.get("removing")
                and isinstance(entry_id := payload.get("entry_id"), str)
                and isinstance(domain := payload.get("domain"), str)
            ):
                if self.hass.config_entries.async_get_entry(
                    entry_id
                ) is None and isinstance(record := payload.get("entry"), dict):
                    # This node never saw the entry: its clean-up needs it.
                    await self._async_apply_record(
                        {"origin": payload.get("origin"), "entry": record},
                        strict=True,
                    )
                removing.append((entry_id, domain))
                continue
            await self._async_apply_record(payload, strict=True)
        for entry_id, domain in removing:
            await self._async_complete_removal(entry_id, domain)

    def _cleanup_failed(self, entry_id: str) -> bool:
        """Return True if the clean-up of the removed entry raised."""
        marker = f"clean-up of config entry {entry_id} "
        return any(
            failure.startswith(marker)
            for failure in cluster_helper.async_transition_failures(self.hass)
        )

    async def _async_complete_removal(self, entry_id: str, domain: str) -> None:
        """Finish a removal the previous leader announced but did not finish.

        Its clean-up may have run partly or not at all: run it here, then
        publish the tombstone.
        """
        if (
            entry := self.hass.config_entries.async_get_entry(entry_id)
        ) is not None and self._replicates(entry.domain):
            _LOGGER.warning(
                "Node %s completes the removal of %s (%s) that the previous"
                " leader started",
                self.node_id,
                entry.title,
                entry.domain,
            )
            self.completing_removals.add(entry_id)
            try:
                await self.hass.config_entries.async_remove(entry_id)
            except Exception:
                _LOGGER.exception("Could not complete the removal of %s", entry_id)
                return
            finally:
                self.completing_removals.discard(entry_id)
            if self._cleanup_failed(entry_id):
                _LOGGER.error(
                    "The clean-up of the removed %s (%s) failed again; the"
                    " removal stays announced",
                    entry.title,
                    entry.domain,
                )
                return
        elif entry is None:
            _LOGGER.warning(
                "Node %s has no copy of the removed entry %s and cannot run its"
                " clean-up",
                self.node_id,
                entry_id,
            )
        self.memory.async_note_removed(entry_id)
        # Published by the flush at the end of the promotion.
        self._deferred_tombstones[entry_id] = domain

    async def _async_reconcile_inventory(self) -> None:
        """Remove known entries whose removal record was lost.

        An entry this node knows from the cluster is removed if the inventory
        lists it as removed, or if neither the inventory nor any record shows
        it any more (the tombstone went with a recreated bucket). Entries made
        on this node only are kept. Without an inventory, nothing is removed;
        if this node saw one before, the user is asked to check.
        """
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            raise HandoverIncomplete("NATS is not connected")
        try:
            current = await kv.get(INVENTORY_KEY)
        except KeyNotFoundError, KeyDeletedError:
            if self.memory.seen_inventory:
                _LOGGER.warning(
                    "The cluster's inventory is missing (the storage bucket was"
                    " lost?); node %s keeps all its entries",
                    self.node_id,
                )
                async_raise_data_lost(self.hass)
            return
        except Exception as err:
            raise HandoverIncomplete(f"could not read the inventory: {err!r}") from err
        try:
            raw = json_loads(current.value or b"")
            check_record(INVENTORY_KEY, raw)
            parsed = parse_inventory(raw)
        except UnsupportedProtocol as err:
            raise HandoverIncomplete(str(err)) from err
        except ValueError:
            parsed = None
        if parsed is None:
            _LOGGER.warning("Ignoring the malformed cluster inventory")
            return
        listed, removed = parsed
        self.memory.async_mark_seen_inventory()
        async_clear_data_lost(self.hass)
        for entry in self.hass.config_entries.async_entries():
            entry_id = entry.entry_id
            if not self._replicates(entry.domain) or entry_id not in self.memory.known:
                continue
            if entry_id not in removed:
                if entry_id in listed:
                    continue
                try:
                    await kv.get(f"{ENTRY_KEY_PREFIX}{entry_id}")
                except KeyNotFoundError, KeyDeletedError:
                    pass  # no record either: the removal was lost
                except Exception as err:
                    raise HandoverIncomplete(
                        f"could not read the entry record {entry_id}: {err!r}"
                    ) from err
                else:
                    continue  # published, not listed yet: it is new
            _LOGGER.warning(
                "%s (%s) was removed on another node; node %s removes it too",
                entry.title,
                entry.domain,
                self.node_id,
            )
            await self.hass.config_entries._async_remove_replica(entry_id)  # noqa: SLF001
            self.memory.async_note_removed(entry_id)

    async def async_mirror(self, key: str, content: str | bytes) -> bool:
        """Publish one store write.

        Returns False if it must be retried: the publication failed, or NATS
        is not connected while this node is the leader.
        """
        if not self.leases.is_leader:
            return True
        term = self.leases.lease_term
        entries_ok = True
        if key == CONFIG_ENTRIES_STORE:
            entries_ok = await self._async_publish_entries()
            if not self.leases.is_leader or self.leases.lease_term != term:
                return True
        if key not in MIRRORED_STORES or key in self.exclude:
            return entries_ok
        kv_key = f"{STORAGE_KEY_PREFIX}{key}"
        if not _valid_kv_key(kv_key):
            return entries_ok
        raw = content.encode() if isinstance(content, str) else content
        digest = hashlib.sha256(raw).digest()
        if self._last_hash.get(kv_key) == digest:
            return entries_ok
        if len(raw) + _ENVELOPE_OVERHEAD > self.coordinator.max_payload:
            if key in self._too_large:
                return entries_ok
            _LOGGER.warning(
                "Store %s (%d bytes) exceeds the NATS max_payload of %d bytes"
                " and is not mirrored; a node that takes over keeps its own"
                " copy. Raise max_payload on the server",
                key,
                len(raw),
                self.coordinator.max_payload,
            )
            if not self.coordinator.connected:
                return False
            # A record of an earlier, smaller version must not be taken for
            # the current one: without a record the next leader keeps its own.
            try:
                await self.coordinator.async_fenced_delete(kv_key)
            except TermSuperseded:
                return True  # no longer this node's to write
            except Exception as err:  # noqa: BLE001 - next write will retry
                self.coordinator.note_error(err)
                return False
            self._too_large.add(key)
            self._last_hash.pop(kv_key, None)
            return entries_ok
        self._too_large.discard(key)
        if not self.coordinator.connected:
            return False
        envelope = b'{"origin":"%s","term":%d,"ts":%f,"content":%s}' % (
            self.node_id.encode(),
            self.coordinator.term,
            time.time(),
            raw,
        )
        try:
            await self.coordinator.async_fenced_put(kv_key, envelope)
        except TermSuperseded:
            return True  # no longer this node's to write
        except Exception as err:  # noqa: BLE001 - next write will retry
            self.coordinator.note_error(err)
            _LOGGER.warning("Could not mirror store %s: %s", key, err)
            return False
        if not self.leases.is_leader or self.leases.lease_term != term:
            return True
        self._last_hash[kv_key] = digest
        return entries_ok

    async def _async_publish_entries(self) -> bool:
        """Publish every entry record; return False if one must be retried."""
        ok = True
        for entry in self.hass.config_entries.async_entries():
            if not await self._async_publish_entry(entry):
                self._pending_entries.add(entry.entry_id)
                ok = False
        return ok

    async def _async_publish_entry(self, entry: ConfigEntry) -> bool:
        """Publish an entry's record if this node holds it and it changed.

        Returns False if the record must be published again later.
        """
        if (
            not self.leases.is_leader
            or not self._replicates(entry.domain)
            or self.leases.is_handover(entry.entry_id)
        ):
            return True
        term = self.leases.lease_term
        record = entry.as_dict()
        kv_key = f"{ENTRY_KEY_PREFIX}{entry.entry_id}"
        digest = _record_digest(record)
        if self._last_hash.get(kv_key) == digest:
            return True
        if not self.coordinator.connected:
            return False
        value = json_bytes(
            self._seal(
                kv_key,
                {
                    "protocol": PROTOCOL_VERSION,
                    "origin": self.node_id,
                    "term": self.coordinator.term,
                    "ts": time.time(),
                    "entry": record,
                },
            )
        )
        if len(value) + _ENVELOPE_OVERHEAD > self.coordinator.max_payload:
            # Never accepted, however often it is tried: retrying would keep
            # the storage flush failing, and with it a clean release.
            if self._entry_too_large.get(kv_key) == digest:
                return True
            _LOGGER.error(
                "The record of config entry %s (%s) is larger than the NATS"
                " max_payload (%s bytes) and is not replicated: another node"
                " cannot take it over. Raise max_payload on the NATS server",
                entry.title,
                entry.domain,
                self.coordinator.max_payload,
            )
            # An earlier, smaller record must not be taken for the current one.
            if not await self._async_mark_oversized(entry, kv_key):
                return False
            self._entry_too_large[kv_key] = digest
            return True
        self._entry_too_large.pop(kv_key, None)
        try:
            await self.coordinator.async_fenced_put(kv_key, value)
        except TermSuperseded:
            return True  # no longer this node's to write
        except Exception as err:  # noqa: BLE001 - the caller queues it again
            self.coordinator.note_error(err)
            _LOGGER.warning("Could not publish entry %s: %s", entry.entry_id, err)
            return False
        await self.memory.async_mark_known(entry.entry_id)
        if (changed_at := self._changed_at.pop(entry.entry_id, None)) is not None:
            # The window in which a change (a rotated token) is lost if this
            # node fails.
            _LOGGER.debug(
                "Published entry %s %.1f ms after it changed",
                entry.entry_id,
                (time.monotonic() - changed_at) * 1000,
            )
        if not self.leases.is_leader or self.leases.lease_term != term:
            return True
        self._last_hash[kv_key] = digest
        return True

    async def _async_mark_oversized(self, entry: ConfigEntry, kv_key: str) -> bool:
        """Replace an earlier record of an entry that no longer fits.

        The record of an earlier, smaller version is out of date: a node that
        took the entry over from it would run with old data (a token that was
        rotated since). A small marker takes its place, and a promotion
        refuses to go on while it stands. Without a record there is nothing to
        replace. Returns False to try again later.
        """
        kv = self.coordinator.kv_storage
        if kv is None:
            return False
        try:
            await kv.get(kv_key)
        except KeyNotFoundError, KeyDeletedError:
            return True  # never published: no node has an older copy to adopt
        except Exception as err:  # noqa: BLE001 - the caller queues it again
            self.coordinator.note_error(err)
            _LOGGER.warning("Could not read the record of %s: %s", entry.entry_id, err)
            return False
        try:
            await self.coordinator.async_fenced_put(
                kv_key,
                json_bytes(
                    {
                        "protocol": PROTOCOL_VERSION,
                        "origin": self.node_id,
                        "term": self.coordinator.term,
                        "ts": time.time(),
                        "oversized": True,
                        "entry_id": entry.entry_id,
                        "domain": entry.domain,
                    }
                ),
            )
        except TermSuperseded:
            return True  # no longer this node's to write
        except Exception as err:  # noqa: BLE001 - the caller queues it again
            self.coordinator.note_error(err)
            _LOGGER.warning("Could not mark %s as too large: %s", entry.entry_id, err)
            return False
        self._last_hash.pop(kv_key, None)
        return True

    async def _async_publish_tombstone(self, entry_id: str, domain: str) -> bool:
        """Publish the removal of an entry; return False to try again later.

        A tombstone is a record under the entry's key, not a delete: a missing
        key must keep meaning "never published".
        """
        if not self._replicates(domain):
            return True
        if not self.leases.is_leader or not self.coordinator.connected:
            return False  # not confirmed: nothing may be reported as done
        try:
            await self.coordinator.async_fenced_put(
                f"{ENTRY_KEY_PREFIX}{entry_id}",
                json_bytes(
                    {
                        "protocol": PROTOCOL_VERSION,
                        "origin": self.node_id,
                        "term": self.coordinator.term,
                        "ts": time.time(),
                        "removed": True,
                        "entry_id": entry_id,
                        "domain": domain,
                    }
                ),
            )
        except TermSuperseded:
            return True  # no longer this node's to write
        except Exception as err:  # noqa: BLE001 - the caller queues it again
            self.coordinator.note_error(err)
            _LOGGER.warning("Could not publish the removal of %s: %s", entry_id, err)
            return False
        return True

    async def _async_pull_entry(self, entry: ConfigEntry) -> None:
        """Adopt the previous owner's view of an entry before running it.

        Then publish this node's view: nothing else does until the entry
        changes, and the record may be missing or older (a lease won at boot,
        or a bucket that was recreated while no lease was held).
        """
        await self._async_adopt_entry(entry)
        self._pending_entries.add(entry.entry_id)
        self._wakeup.set()

    async def _async_adopt_entry(self, entry: ConfigEntry) -> None:
        if not self.leases.is_handover(entry.entry_id):
            # This node kept the lease: its own copy is the newest.
            return
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            raise HandoverIncomplete("NATS is not connected")
        try:
            current = await kv.get(f"{ENTRY_KEY_PREFIX}{entry.entry_id}")
        except KeyNotFoundError, KeyDeletedError:
            return  # never published (new entry, or first cluster boot)
        except Exception as err:
            # A timeout is not "no record": the previous owner may have
            # rotated credentials that this node's copy does not have.
            raise HandoverIncomplete(
                f"could not read the entry record: {err!r}"
            ) from err
        kv_key = f"{ENTRY_KEY_PREFIX}{entry.entry_id}"
        try:
            raw = json_loads(current.value or b"")
            check_record(kv_key, raw)
            payload: dict[str, Any] = self._open(kv_key, raw)
            if payload.get("oversized"):
                raise HandoverIncomplete(_oversized_message(kv_key))
            record: dict[str, Any] = payload["entry"]
        except (RecordKeyError, UnsupportedProtocol) as err:
            raise HandoverIncomplete(str(err)) from err
        except ValueError, KeyError, TypeError:
            _LOGGER.warning("Ignoring malformed handover record for %s", entry.entry_id)
            return
        changes = {
            f: record[f]
            for f in _ENTRY_FIELDS
            # ``async_update_entry`` cannot change ``disabled_by``.
            if f != "disabled_by" and f in record and record[f] != getattr(entry, f)
        }
        if not changes:
            return
        _LOGGER.info(
            "Node %s adopting %s for %s from node %s before setup",
            self.node_id,
            ", ".join(sorted(changes)),
            entry.title,
            payload.get("origin"),
        )
        # Remember the hash so our own save of this change is not re-published
        # needlessly; it will be published once we write anything new.
        self._last_hash[f"{ENTRY_KEY_PREFIX}{entry.entry_id}"] = _record_digest(record)
        self.hass.config_entries.async_update_entry(entry, **changes)

    async def _async_watch_entries(self) -> None:
        """Apply the leader's entry records while this node is a standby."""
        while True:
            try:
                kv = self.coordinator.kv_storage
                if TYPE_CHECKING:
                    assert kv is not None
                # Replays every current record when it starts: that brings a
                # node that starts late up to date.
                watcher = await kv.watch(f"{ENTRY_KEY_PREFIX}*")
                try:
                    async for update in watcher:
                        if update is None or update.operation in ("DEL", "PURGE"):
                            continue
                        if LEADER_KEY in self.leases.held:
                            # This node leads, or is being promoted (the
                            # promotion reads everything): its copy is the
                            # newest.
                            continue
                        try:
                            payload = json_loads(update.value or b"")
                        except ValueError:
                            _LOGGER.warning(
                                "Ignoring malformed entry record %s", update.key
                            )
                            continue
                        try:
                            # Before anything of it is used, the term included.
                            check_record(update.key, payload)
                            payload = self._open(update.key, payload)
                        except (RecordKeyError, UnsupportedProtocol) as err:
                            # A standby cannot apply it; a promotion refuses
                            # to go on without it (see ``_async_apply_all``).
                            if update.key not in self._unreadable:
                                self._unreadable.add(update.key)
                                _LOGGER.error("Cannot read an entry record: %s", err)
                            continue
                        self._unreadable.discard(update.key)
                        await self._async_apply_record(payload)
                finally:
                    await stop_watcher(watcher)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                self.coordinator.note_error(err)
                _LOGGER.debug("Entry watcher restarting: %s", err)
            await asyncio.sleep(1)

    async def _async_apply_record(self, payload: Any, *, strict: bool = False) -> None:
        """Apply a published entry record or tombstone to this node.

        A record whose content is wrong is skipped, loudly: it is wrong on every
        retry, and nothing was changed. ``strict`` (a promotion): any other
        failure raises ``HandoverIncomplete``, because applying the record may
        have changed something and the node would lead with entries that differ
        from the leader's.
        """
        async with self._apply_lock:
            try:
                _validate_record(payload)
            except DATA_ERRORS as err:
                _LOGGER.error("Not applying a config entry record: %r", err)
                return
            try:
                await self._async_apply(payload)
            except Exception as err:
                if strict:
                    raise HandoverIncomplete(
                        f"could not apply a config entry record: {err!r}"
                    ) from err
                _LOGGER.warning("Could not apply a config entry record: %r", err)

    async def _async_apply(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            raise TypeError("the record is not an object")
        self.coordinator.note_term(payload.get("term"))
        if payload.get("origin") == self.node_id:
            return
        config_entries = self.hass.config_entries
        if payload.get("removing"):
            # The leader announced a removal; its tombstone follows. If it
            # does not, the node that takes over completes the removal.
            return
        if payload.get("oversized"):
            # Nothing to apply; a promotion refuses to go on (see
            # ``_async_apply_all``).
            return
        if payload.get("removed"):
            entry_id = payload["entry_id"]
            if (
                entry := config_entries.async_get_entry(entry_id)
            ) is not None and self._replicates(entry.domain):
                # The leader ran the integration's clean-up already. An entry
                # of a domain this node keeps local is not the leader's.
                await config_entries._async_remove_replica(entry_id)  # noqa: SLF001
                self.memory.async_note_removed(entry_id)
            self._last_hash.pop(f"{ENTRY_KEY_PREFIX}{entry_id}", None)
            return

        record: dict[str, Any] = payload["entry"]
        entry_id = record["entry_id"]
        if not self._replicates(record["domain"]):
            return
        digest = _record_digest(record)
        kv_key = f"{ENTRY_KEY_PREFIX}{entry_id}"

        await self.memory.async_mark_known(entry_id)
        if (entry := config_entries.async_get_entry(entry_id)) is None:
            # Built as ``ConfigEntries.async_initialize`` builds an entry.
            self._last_hash[kv_key] = digest
            await config_entries.async_add(
                ConfigEntry(
                    created_at=datetime.fromisoformat(record["created_at"]),
                    data=record["data"],
                    disabled_by=try_parse_enum(
                        ConfigEntryDisabler, record["disabled_by"]
                    ),
                    discovery_keys=MappingProxyType(
                        {
                            domain: tuple(
                                DiscoveryKey.from_json_dict(key) for key in keys
                            )
                            for domain, keys in record["discovery_keys"].items()
                        }
                    ),
                    domain=record["domain"],
                    entry_id=entry_id,
                    minor_version=record["minor_version"],
                    modified_at=datetime.fromisoformat(record["modified_at"]),
                    options=record["options"],
                    pref_disable_new_entities=record["pref_disable_new_entities"],
                    pref_disable_polling=record["pref_disable_polling"],
                    source=record["source"],
                    subentries_data=record["subentries"],
                    title=record["title"],
                    unique_id=record["unique_id"],
                    version=record["version"],
                )
            )
            return

        changes = {
            f: record[f]
            for f in _ENTRY_FIELDS
            # ``async_update_entry`` cannot change ``disabled_by``.
            if f != "disabled_by" and f in record and record[f] != getattr(entry, f)
        }
        if changes:
            config_entries.async_update_entry(entry, **changes)
        self._async_apply_subentries(entry, record.get("subentries", []))
        disabled_by = try_parse_enum(ConfigEntryDisabler, record["disabled_by"])
        self._last_hash[kv_key] = digest
        if entry.disabled_by is not disabled_by:
            self.applying_record = True
            try:
                await config_entries.async_set_disabled_by(entry_id, disabled_by)
            finally:
                self.applying_record = False

    @callback
    def _async_apply_subentries(
        self, entry: ConfigEntry, records: list[dict[str, Any]]
    ) -> None:
        config_entries = self.hass.config_entries
        remote = {record["subentry_id"]: record for record in records}
        for subentry_id in set(entry.subentries) - set(remote):
            config_entries.async_remove_subentry(entry, subentry_id)
        for subentry_id, record in remote.items():
            if (subentry := entry.subentries.get(subentry_id)) is None:
                config_entries.async_add_subentry(
                    entry,
                    ConfigSubentry(
                        data=MappingProxyType(record["data"]),
                        subentry_id=subentry_id,
                        subentry_type=record["subentry_type"],
                        title=record["title"],
                        unique_id=record["unique_id"],
                    ),
                )
            elif (
                subentry.data != record["data"]
                or subentry.title != record["title"]
                or subentry.unique_id != record["unique_id"]
            ):
                config_entries.async_update_subentry(
                    entry,
                    subentry,
                    data=record["data"],
                    title=record["title"],
                    unique_id=record["unique_id"],
                )


def _read_file(path: str) -> bytes | None:
    try:
        with open(path, "rb") as file:
            return file.read()
    except OSError:
        return None
