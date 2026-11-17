"""Replicate the state machine through the ``hass_states`` bucket.

Loop freedom is by construction rather than by tagging contexts:

* A node publishes only the entities it *owns*: entities of config entries it
  holds the lease for. Entities without a config entry (YAML) and entities
  of config entries that run on every node exist on every node, so they are
  not published.
* A node never applies a remote state over an entity that is live locally,
  and ignores updates that carry its own node id as origin.

So a mirrored state can never be republished.
"""

import asyncio
from contextlib import suppress
import logging
from typing import TYPE_CHECKING, Any

from homeassistant.const import EVENT_STATE_CHANGED
from homeassistant.core import (
    Context,
    Event,
    EventStateChangedData,
    HomeAssistant,
    State,
    callback,
)
from homeassistant.helpers import cluster as cluster_helper, entity_registry as er
from homeassistant.helpers.entity_platform import DATA_DOMAIN_ENTITIES
from homeassistant.helpers.json import json_bytes
from homeassistant.util.json import json_loads_object

from .const import LEADER_KEY
from .coordinator import ClusterCoordinator, stop_watcher
from .lease_manager import LeaseManager

_LOGGER = logging.getLogger(__name__)

_DELETE = b""


class StateReplicator:
    """Publishes owned states and mirrors remote ones."""

    def __init__(
        self,
        hass: HomeAssistant,
        node_id: str,
        coordinator: ClusterCoordinator,
        lease_manager: LeaseManager,
    ) -> None:
        """Initialize the replicator."""
        self.hass = hass
        self.node_id = node_id
        self.coordinator = coordinator
        self.leases = lease_manager
        # entity_id -> (origin node, config_entry_id, integration) for states
        # applied from KV
        self.mirrored: dict[str, tuple[str, str | None, str | None]] = {}
        self._pending: dict[str, bytes] = {}
        # entity_id -> config entry it was published for (None for an entity
        # of a YAML or discovered platform, which has none). A removed or
        # renamed entity no longer resolves to its entry when its removal is
        # announced, so its deletion is authorized by this record instead.
        self._published: dict[str, str | None] = {}
        # key -> revision of this node's last write (the delete is conditional on it)
        self._revisions: dict[str, int] = {}
        self._wakeup = asyncio.Event()
        self._suppress: set[str] = set()
        self._unsub: Any = None
        self._tasks: list[asyncio.Task[None]] = []
        self._watch_task: asyncio.Task[None] | None = None
        self._entity_registry: er.EntityRegistry | None = None
        self._mirroring_suspended = False

    async def async_start(self) -> None:
        """Start publishing and mirroring."""
        self._entity_registry = er.async_get(self.hass)
        self._unsub = self.hass.bus.async_listen(
            EVENT_STATE_CHANGED, self._async_on_state_changed
        )
        self.leases.add_before_setup(self._async_before_entry_setup)
        self.leases.add_after_demote(self._async_after_demote)
        self._tasks = [
            self.hass.async_create_background_task(
                self._async_flush_loop(), f"cluster {self.node_id} state publisher"
            )
        ]
        self._start_watcher()
        # Publish what we already own (states created before we loaded).
        for state in self.hass.states.async_all():
            if self._owns(state.entity_id):
                self._queue(state.entity_id, state)

    async def async_stop(self) -> None:
        """Stop all tasks."""
        if self._unsub:
            self._unsub()
            self._unsub = None
        for task in list(self._tasks):
            task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        if self._watch_task is not None:
            self._watch_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._watch_task
            self._watch_task = None

    @callback
    def _start_watcher(self) -> None:
        """(Re)start the KV watcher; a new watcher redelivers every key."""
        if self._watch_task is not None:
            self._watch_task.cancel()
        self._watch_task = self.hass.async_create_background_task(
            self._async_watch_loop(), f"cluster {self.node_id} state mirror"
        )

    def entry_of(self, entity_id: str) -> str | None:
        """Config entry behind an entity, as far as this node can tell.

        Look up using entity registry, then via live entity platform.
        """
        if (
            self._entity_registry is not None
            and (reg := self._entity_registry.async_get(entity_id)) is not None
            and reg.config_entry_id is not None
        ):
            return reg.config_entry_id

        domain = entity_id.partition(".")[0]
        entity = (
            self.hass.data.get(DATA_DOMAIN_ENTITIES, {}).get(domain, {}).get(entity_id)
        )
        if (
            entity is not None
            and entity.platform is not None
            and entity.platform.config_entry is not None
        ):
            return entity.platform.config_entry.entry_id
        return None

    def _platform_of(self, entity_id: str) -> Any:
        domain = entity_id.partition(".")[0]
        entity = (
            self.hass.data.get(DATA_DOMAIN_ENTITIES, {}).get(domain, {}).get(entity_id)
        )
        return entity.platform if entity is not None else None

    def _owns(self, entity_id: str) -> bool:
        """Return True for an entity this node publishes as the leader.

        That is an entity of a config entry this node holds, or an entity of a
        platform set up from YAML or by discovery, which runs on the leader
        only.
        """
        if not self.leases.is_leader or entity_id in self.mirrored:
            return False
        if (entry_id := self.entry_of(entity_id)) is not None:
            return self.leases.holds(entry_id)
        return self._is_yaml_platform_entity(entity_id)

    def _is_yaml_platform_entity(self, entity_id: str) -> bool:
        """Return True for a live entity of a gated YAML or discovered platform."""
        platform = self._platform_of(entity_id)
        return (
            platform is not None
            and platform.config_entry is None
            and platform.platform_name != platform.domain
            and not cluster_helper.async_domain_runs_on_every_instance(
                self.hass, platform.platform_name
            )
        )

    def _may_delete(self, entity_id: str) -> bool:
        """Return True if this node may delete the published ``entity_id``.

        What this node published, it may delete, also when the entry was
        disabled or removed meanwhile (``holds`` is False then, and the state
        would stay on every standby for good). The delete is conditional on
        the revision of this node's own write, so it cannot remove a state that
        a newer leader published.
        """
        if entity_id in self._published:
            return True
        return self._owns(entity_id)

    def _may_write(self, entity_id: str, value: bytes) -> bool:
        if value is _DELETE:
            return self._may_delete(entity_id)
        return self._owns(entity_id)

    def owner_of(self, entity_id: str) -> str | None:
        """Origin node of a mirrored entity, if any."""
        if (info := self.mirrored.get(entity_id)) is None:
            return None
        return info[0]

    def _live_locally(self, entity_id: str) -> bool:
        domain = entity_id.partition(".")[0]
        return entity_id in self.hass.data.get(DATA_DOMAIN_ENTITIES, {}).get(domain, {})

    @callback
    def _async_on_state_changed(self, event: Event[EventStateChangedData]) -> None:
        entity_id = event.data["entity_id"]
        if entity_id in self._suppress:
            return
        new_state = event.data["new_state"]
        if not (
            self._may_delete(entity_id) if new_state is None else self._owns(entity_id)
        ):
            return
        self._queue(entity_id, new_state)

    @callback
    def _queue(self, entity_id: str, state: State | None) -> None:
        if state is None:
            self._pending[entity_id] = _DELETE
        else:
            entry_id = self.entry_of(entity_id)
            self._published[entity_id] = entry_id
            platform = self._platform_of(entity_id)
            integration = platform.platform_name if platform is not None else None
            self._pending[entity_id] = (
                b'{"origin":"%s","term":%d,"config_entry_id":%s,"integration":%s,'
                b'"state":%s}'
                % (
                    self.node_id.encode(),
                    self.coordinator.term,
                    _json_str(entry_id),
                    _json_str(integration),
                    state.as_dict_json,
                )
            )
        self._wakeup.set()

    async def _async_flush_loop(self) -> None:
        """Drain the coalescing queue: per key, only the newest value is sent."""
        while True:
            await self._wakeup.wait()
            self._wakeup.clear()
            while self._pending:
                # Read each time: the bucket is replaced if it is recreated.
                kv = self.coordinator.kv_states
                if TYPE_CHECKING:
                    assert kv is not None
                batch, self._pending = self._pending, {}
                # Ownership may have moved while writes were queued (e.g. we
                # were partitioned and fenced); never publish stale states.
                batch = {k: v for k, v in batch.items() if self._may_write(k, v)}
                results = await asyncio.gather(
                    *(
                        self._async_delete(kv, key)
                        if value is _DELETE
                        else kv.put(key, value)
                        for key, value in batch.items()
                    ),
                    return_exceptions=True,
                )
                failed = {
                    key: value
                    for (key, value), result in zip(batch.items(), results, strict=True)
                    if isinstance(result, Exception)
                }
                for (key, value), result in zip(batch.items(), results, strict=True):
                    if value is _DELETE:
                        if key not in failed:
                            self._published.pop(key, None)
                            self._revisions.pop(key, None)
                    elif isinstance(result, int):
                        self._revisions[key] = result
                if failed:
                    _LOGGER.debug("Re-queueing %d state writes", len(failed))
                    for result in results:
                        if isinstance(result, Exception):
                            self.coordinator.note_error(result)
                            break
                    for key, value in failed.items():
                        self._pending.setdefault(key, value)
                    await asyncio.sleep(0.5)

    async def _async_delete(self, kv: Any, key: str) -> None:
        """Delete a state this node published, unless it was replaced since.

        Conditional on the revision of this node's own last write: a newer
        leader's state of the same key is not deleted (the refusal is the
        answer, not an error).
        """
        last = self._revisions.get(key)
        try:
            await kv.delete(key, last=last)
        except Exception as err:
            if not self.coordinator.is_refused_write(err):
                raise
            _LOGGER.debug("Not deleting %s: it was written since", key)

    async def _async_watch_loop(self) -> None:
        while True:
            try:
                kv = self.coordinator.kv_states
                if TYPE_CHECKING:
                    assert kv is not None
                watcher = await kv.watchall()
                try:
                    async for update in watcher:
                        if update is not None:
                            self._async_apply(
                                update.key, update.operation, update.value
                            )
                finally:
                    # Cancelling the task alone leaves the consumer and the
                    # subscription behind, one more per restart.
                    await stop_watcher(watcher)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                self.coordinator.note_error(err)
                _LOGGER.debug("State watcher restarting: %s", err)
            await asyncio.sleep(1)

    @callback
    def _async_apply(
        self, key: str, operation: str | None, value: bytes | None
    ) -> None:
        if self._mirroring_suspended:
            return
        if operation in ("DEL", "PURGE"):
            if key in self.mirrored and not self._live_locally(key):
                self.mirrored.pop(key)
                self._remove_silently(key)
            return
        try:
            payload: dict[str, Any] = json_loads_object(value or b"")
            origin: str = payload["origin"]
            remote = State.from_dict(payload["state"])
        except ValueError, KeyError, TypeError:
            _LOGGER.warning("Ignoring malformed state payload for %s", key)
            return
        if remote is None or origin == self.node_id:
            return
        if self.coordinator.is_stale_term(term := payload.get("term")):
            # Written by a leader that was superseded meanwhile.
            _LOGGER.debug("Ignoring a state of the older term %s", term)
            return
        self.coordinator.note_term(term)
        entity_id = remote.entity_id
        if self._live_locally(entity_id):
            return  # never let a remote write clobber something we run
        # Mirror only what this node will not run itself: entities of a
        # leased entry held elsewhere (or unknown here). YAML entities and
        # entities of unleased entries exist on every node; mirroring them
        # would squat their ids before the local integration loads, and the
        # local entity would come up renamed (``..._2``).
        entry_id = payload.get("config_entry_id")
        if not entry_id:
            # An entity of a YAML or discovered platform of the leader: it is
            # not live here (the platform runs on the leader only).
            integration = payload.get("integration")
            if (
                not integration
                or LEADER_KEY in self.leases.held
                or cluster_helper.async_domain_runs_on_every_instance(
                    self.hass, integration
                )
            ):
                return
        elif self.leases.holds(entry_id):
            return
        entry = self.hass.config_entries.async_get_entry(entry_id) if entry_id else None
        if entry is not None and not self.leases.gates(entry):
            return
        if (
            entry_id
            and entry is None
            and not self.leases.domain_is_clustered(payload.get("integration") or "")
        ):
            # Another node's own entry of a domain that runs on every node
            # (an older node may still publish those).
            return
        if entity_id not in self.mirrored:
            _LOGGER.debug(
                "Node %s mirroring %s from %s (entry %s)",
                self.node_id,
                entity_id,
                origin,
                payload.get("config_entry_id"),
            )
        self.mirrored[entity_id] = (
            origin,
            payload.get("config_entry_id"),
            payload.get("integration"),
        )
        ctx = remote.context
        self._suppress.add(entity_id)
        try:
            self.hass.states.async_set(
                entity_id,
                remote.state,
                remote.attributes,
                context=Context(
                    user_id=ctx.user_id, parent_id=ctx.parent_id, id=ctx.id
                ),
                timestamp=remote.last_updated_timestamp,
            )
        finally:
            self._suppress.discard(entity_id)

    @callback
    def _remove_silently(self, entity_id: str) -> None:
        self._suppress.add(entity_id)
        try:
            self.hass.states.async_remove(entity_id)
        finally:
            self._suppress.discard(entity_id)

    async def async_before_promote(self) -> None:
        """Suspend mirroring and drop all mirrored states before loading entries as leader."""
        self._mirroring_suspended = True
        for entity_id in list(self.mirrored):
            self.mirrored.pop(entity_id, None)
            self._remove_silently(entity_id)

    @callback
    def async_resume_mirroring(self) -> None:
        """Resume mirroring if promotion was aborted."""
        self._mirroring_suspended = False
        self._start_watcher()

    async def async_after_demote(self, entry_id: str | None = None) -> None:
        """We stopped running as leader: drop published records and resync mirror."""
        self._mirroring_suspended = False
        self._published.clear()
        self._start_watcher()

    async def _async_before_entry_setup(self, entry: Any) -> None:
        """Drop mirrored states of an entry we are about to run.

        Otherwise the entity platform finds the id taken and logs
        "Entity id already exists - ignoring", and the entity never loads.
        """
        if TYPE_CHECKING:
            assert self._entity_registry is not None
        ids = {
            entity_id
            for entity_id, (_, entry_id, integration) in self.mirrored.items()
            if entry_id == entry.entry_id
            or (entry_id is None and integration == entry.domain)
        } | {
            reg.entity_id
            for reg in er.async_entries_for_config_entry(
                self._entity_registry, entry.entry_id
            )
            if reg.entity_id in self.mirrored
        }
        _LOGGER.debug(
            "Node %s dropping %d mirrored states of %s before setup: %s",
            self.node_id,
            len(ids),
            entry.entry_id,
            sorted(ids),
        )
        for entity_id in ids:
            self.mirrored.pop(entity_id, None)
            self._remove_silently(entity_id)

    async def _async_after_demote(self, entry_id: str | None = None) -> None:
        """We stopped running an entry: pick up its owner's current states."""
        if entry_id is not None and not self.leases.is_leader:
            return
        await self.async_after_demote(entry_id)

    @callback
    def async_resync(self) -> None:
        """Publish and mirror everything again (the bucket was recreated)."""
        self._start_watcher()
        self.publish_owned()

    @callback
    def publish_owned(self) -> None:
        """Queue every owned state, e.g. after taking over publishing."""
        self._mirroring_suspended = False
        for state in self.hass.states.async_all():
            if state.entity_id not in self.mirrored and self._owns(state.entity_id):
                self._queue(state.entity_id, state)


def _json_str(value: str | None) -> bytes:
    """Encode an identifier (entry id, integration domain) as a JSON value."""
    return json_bytes(value)
