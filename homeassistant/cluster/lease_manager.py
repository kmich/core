"""Single-leader lease management and warm standby lifecycle.

In Single-Leader mode, there is exactly one cluster lease: ``_leader``.
Every gated ``ConfigEntry`` runs on the leader node alongside automations
and scripts. Other nodes stay warm on standby and take over if the leader
fails or disconnects. Each node also renews ``_node.<node_id>`` for membership
and duplicate ``node_id`` detection.
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
import logging
from time import monotonic
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from nats.js.errors import KeyNotFoundError, KeyWrongLastSequenceError, NoKeysError
from nats.js.kv import KV_DEL, KeyValue
import probatio

from homeassistant import loader
from homeassistant.components.homeassistant.const import DATA_STOP_HANDLER
from homeassistant.config_entries import (
    SOURCE_IGNORE,
    ConfigEntry,
    ConfigEntryState,
    OperationNotAllowed,
)
from homeassistant.const import RESTART_EXIT_CODE, __version__ as HA_VERSION
from homeassistant.core import CoreState, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import cluster as cluster_helper
from homeassistant.helpers.json import json_bytes
from homeassistant.util.json import json_loads

from .const import (
    FAILED_SETUP_HANDOFF_AFTER,
    GRACE_KEY_PREFIX,
    HANDOFF_COOLDOWN,
    LEADER_KEY,
    MEMBER_KEY_PREFIX,
    PROTOCOL_VERSION,
)
from .coordinator import ClusterCoordinator, stop_watcher
from .protocol import is_supported, version_of
from .watchdog import StopWatchdog

if TYPE_CHECKING:
    from .state_replicator import StateReplicator
    from .storage_sync import StorageSync

_LOGGER = logging.getLogger(__name__)

_INACTIVE_STATES = (ConfigEntryState.NOT_LOADED, ConfigEntryState.MIGRATION_ERROR)
_RUNNING_STATES = (
    ConfigEntryState.LOADED,
    ConfigEntryState.FAILED_UNLOAD,
    ConfigEntryState.UNLOAD_IN_PROGRESS,
    ConfigEntryState.SETUP_IN_PROGRESS,
)
_FAILING_STATES = (ConfigEntryState.SETUP_RETRY, ConfigEntryState.SETUP_ERROR)

_UNKNOWN_HOLDER = "?"

# A promotion that fails this often in a row (or a handover that cannot be
# completed at once) is shown as a repair issue, not only in the log.
PROMOTION_FAILURES_BEFORE_ISSUE = 3

STANDBY_DUPLICATE_NODE_ID = (
    "Cluster standby: another instance runs with the same node_id"
)
STANDBY_SHUTTING_DOWN = "Cluster standby: this node is shutting down"
STANDBY_SETUP_BLOCKED = (
    "Cluster standby: a step before the setup failed on this node (see the log)"
)
STANDBY_SETUP_FAILED = (
    "Cluster standby: setup failed on this node; another node may run it"
)


class HandoverIncomplete(Exception):
    """A pre-setup hook could not finish; the entry must not start yet."""


class ApplyFailed(Exception):
    """Applying a record of the leader failed after it was accepted as valid.

    Unlike a record with wrong content, it may have changed something.
    """


class _Undecided(Exception):
    """Whether the lease is still held could not be found out (try again)."""


# A record whose content is wrong (malformed, an unexpected shape, an invalid
# value) is wrong on every retry: it is skipped, loudly. Any other failure
# while a promotion applies the leader's data (I/O, a time-out, an error of
# Core) may pass, so the promotion does not go on until it does.
DATA_ERRORS: tuple[type[Exception], ...] = (
    ValueError,
    KeyError,
    TypeError,
    AttributeError,
    probatio.Invalid,
)


@dataclass(slots=True)
class _Lease:
    revision: int
    sent_at: float
    fence: asyncio.TimerHandle | None = None
    grace_until: float | None = None


def _fence_after(ttl: float, renew_interval: float = 1.0) -> float:
    return ttl - max(renew_interval * 0.6, ttl * 0.2, 0.5)


class LeaseManager:
    """Acquires, renews, fences, and manages the single cluster leader lease."""

    def __init__(
        self,
        hass: HomeAssistant,
        node_id: str,
        coordinator: ClusterCoordinator,
        *,
        ttl: float,
        renew_interval: float,
        unload_grace: float,
        exclude_domains: set[str] | frozenset[str] = frozenset(),
    ) -> None:
        """Initialize the lease manager."""
        self.hass = hass
        self.node_id = node_id
        self.coordinator = coordinator
        self.ttl = ttl
        self.renew_interval = renew_interval
        self.unload_grace = unload_grace
        self.fence_after = _fence_after(ttl, renew_interval)
        self.incarnation = uuid4().hex
        self.member_key = f"{MEMBER_KEY_PREFIX}{node_id}"
        self._exclude = frozenset(exclude_domains)

        self._held: dict[str, _Lease] = {}
        self._is_leader = False
        # The lease term whose promotion set up the clustered entries (see
        # ``serving``).
        self._served_term: int | None = None
        # Clustered entries a promotion (or the tick, for entries that were
        # waiting) is setting up now.
        self._promoted_setups: set[str] = set()
        self._is_handover = False
        self._handover: set[str] = set()
        self._standby: set[str] = set()
        # Entries whose pre-setup hook failed unexpectedly (logged once).
        self._setup_blocked: set[str] = set()
        self._owners: dict[str, tuple[str, float]] = {}
        self._peer_incarnations: dict[str, str] = {}
        # Key -> (node, protocol version, when seen) of every key a node wrote.
        self._peer_versions: dict[str, tuple[str, Any, float]] = {}
        # Key -> (node, Home Assistant version, when seen), for the nodes that
        # say which version they run (the repair issue; it never blocks).
        self._peer_ha_versions: dict[str, tuple[str, str, float]] = {}
        # The last member key of each other node and when it was seen; kept
        # after the key expired (what a node said last before it went away).
        self._peer_members: dict[str, tuple[dict[str, Any], float]] = {}
        # Extra fields of this node's member key (see ``member_extra``).
        self.member_extra: Callable[[], dict[str, Any]] | None = None
        self._ha_version_mismatch: frozenset[tuple[str, str]] = frozenset()
        # Why this node cannot take over (repair issue), if it is shown.
        self._promotion_failures = 0
        self._cannot_take_over = False
        self._version_blocked: frozenset[str] = frozenset()
        self._demoting: set[str] = set()
        self._handing_off = False
        self._failing_since: float | None = None
        self._cooldown_until: float = 0.0

        self._lease_term: int = 0
        self._fence_deadline: float = 0.0
        self._grace_until: float = 0.0
        # Loop times the takeover grace counts from (see ``_grace_target``).
        self._grace_base: tuple[float, float] | None = None
        self._election_lock = asyncio.Lock()
        self._promotion_task: asyncio.Task[None] | None = None
        self._member_revision: int | None = None

        self._before_setup: list[Callable[[ConfigEntry], Awaitable[None]]] = []
        self._after_demote: list[Callable[[str], Awaitable[None]]] = []
        self._leader_listeners: list[Callable[[bool], Awaitable[None]]] = []
        self._work_trackers: list[
            tuple[Callable[[], bool], Callable[[float], Awaitable[None]]]
        ] = []

        self._loop_task: asyncio.Task[None] | None = None
        self._watch_task: asyncio.Task[None] | None = None
        self._leader_notify: asyncio.Task[None] | None = None

        self._stopping = False
        self._resigned = False
        self._enforcing = False
        self._fatal = False
        self._restart_task: asyncio.Task[None] | None = None

        self._duplicate = False
        self._conflict_until = 0.0
        self._foreign: dict[str, int] = {}
        self._member_since: int | None = None
        self._started_at = 0.0
        # When this node started to watch the lease bucket (monotonic).
        self._observing_since = 0.0
        self._last_own_renewal = 0.0
        self._watchdog = StopWatchdog(f"Node {node_id}")
        # The largest unload grace another node advertised: a node that takes
        # over must wait at least as long as the old leader may still stop.
        self._peer_grace = 0.0
        self._grace_mismatch_logged = False

        self.replicator: StateReplicator | None = None
        self.storage: StorageSync | None = None
        # The node this one last took the lead from, if it is known, and the
        # last other node seen leading.
        self.previous_leader: str | None = None
        self._last_leader_seen: str | None = None

    @property
    def lease_term(self) -> int:
        """Current leadership lease term generation."""
        return self._lease_term

    @property
    def fence_deadline(self) -> float:
        """Monotonic deadline after which leadership expires locally."""
        return self._fence_deadline

    def peer_incarnation(self, node: str) -> str | None:
        """Incarnation UUID of a known cluster peer."""
        return self._peer_incarnations.get(node)

    @property
    def _reclaim_after(self) -> float:
        """How long after start this node may take its own old lease back.

        The earlier run of this node may still be stopping its work for a full
        lease and the unload grace.
        """
        return self.ttl + self.takeover_grace

    @property
    def exclude_domains(self) -> frozenset[str]:
        """Excluded domains configured for this cluster node."""
        return self._exclude

    @property
    def resigned(self) -> bool:
        """Return True once this node stopped for good (shutdown)."""
        return self._resigned

    @property
    def is_leader(self) -> bool:
        """Return True if this node currently holds the leader lease."""
        return (
            LEADER_KEY in self._held
            and self._is_leader
            and monotonic() < self._fence_deadline
        )

    @property
    def leads(self) -> bool:
        """Return True if this node currently acts as leader."""
        return self.is_leader and cluster_helper.async_is_leader(self.hass)

    @property
    def serving(self) -> bool:
        """Return True once this node leads and its promotion set up the entries.

        A new leader acts as leader (``leads``) before it set up the clustered
        entries, one after another, which takes a while on a real
        installation. Inbound traffic should only move to it after that.
        """
        return (
            self.leads
            and self._served_term == self._lease_term
            and not self._promoted_setups
        )

    @property
    def held(self) -> set[str]:
        """Lease keys currently held by this node."""
        if not self._is_leader:
            return set(self._held)
        return {LEADER_KEY} | {
            e.entry_id
            for e in self.hass.config_entries.async_entries()
            if self.gates(e)
        }

    @property
    def standby(self) -> set[str]:
        """Entry ids waiting on standby."""
        return set(self._standby)

    @property
    def members(self) -> set[str]:
        """Other nodes that renewed their member key recently."""
        return {
            node
            for key in self._owners
            if key.startswith(MEMBER_KEY_PREFIX)
            and (node := self.remote_owner(key)) is not None
        }

    @property
    def known_nodes(self) -> set[str]:
        """Other nodes seen recently."""
        return {
            node for key in self._owners if (node := self.remote_owner(key)) is not None
        }

    def holds(self, key: str) -> bool:
        """Return True if this node holds the lease for ``key``."""
        if not self.is_leader:
            return False
        if key == LEADER_KEY:
            return True
        entry = self.hass.config_entries.async_get_entry(key)
        if entry is not None and not self.gates(entry):
            return False
        return True

    def is_handover(self, entry_id: str) -> bool:
        """Return True if the entry is undergoing failover handover."""
        return self._is_handover or entry_id in self._handover

    def domain_is_clustered(self, domain: str) -> bool:
        """Return True if config entries of ``domain`` run on the leader only."""
        if domain in self._exclude or domain in cluster_helper.EVERY_INSTANCE_DOMAINS:
            return False
        try:
            integration = loader.async_get_loaded_integration(self.hass, domain)
        except loader.IntegrationNotLoaded:
            return True
        return integration.integration_type != "system"

    def gates(self, entry: ConfigEntry) -> bool:
        """Return True if ``entry`` runs only on the cluster leader."""
        if entry.disabled_by:
            return False
        return self._tracks(entry)

    def _tracks(self, entry: ConfigEntry) -> bool:
        """Return True if ``entry`` may run only while this node leads.

        Unlike ``gates`` this includes disabled entries: one that was disabled
        while it ran still runs until its unload finished.
        """
        if entry.source == SOURCE_IGNORE or entry.domain in self._exclude:
            return False
        return not cluster_helper.async_entry_runs_on_every_instance(self.hass, entry)

    @property
    def takeover_grace(self) -> float:
        """How long a new leader waits after a lease that was not released.

        The largest unload grace of this node and of every peer that
        advertised one: the old leader may stop its work for that long.
        """
        return max(self.unload_grace, self._peer_grace)

    @callback
    def _async_note_peer_grace(self, node: str, grace: Any) -> None:
        if node == self.node_id:
            return
        if not isinstance(grace, (int, float)) or isinstance(grace, bool):
            return
        if grace > self._peer_grace:
            self._peer_grace = float(grace)
        if grace != self.unload_grace and not self._grace_mismatch_logged:
            self._grace_mismatch_logged = True
            _LOGGER.warning(
                "Node %s has an unload grace of %ss and node %s one of %ss;"
                " configure the same timing on every node. Until then a node"
                " that takes over waits for the larger one",
                node,
                grace,
                self.node_id,
                self.unload_grace,
            )

    def _is_quiescent(self) -> bool:
        """Return True if nothing that runs on the leader only runs here.

        Only then may this node release the lease cleanly, which lets another
        node take over without waiting for the lease to expire.
        """
        if self._fatal or self._demoting:
            return False
        if cluster_helper.async_transitions_pending(self.hass):
            return False
        if any(busy() for busy, _ in self._work_trackers):
            return False
        return not any(
            e.state not in _INACTIVE_STATES
            for e in self.hass.config_entries.async_entries()
            if self._tracks(e)
        )

    def remote_owner(self, key: str) -> str | None:
        """Node last seen renewing ``key``, if the sighting is still fresh."""
        if (seen := self._owners.get(key)) is None:
            return None
        node, at = seen
        if node == self.node_id or monotonic() - at > self.ttl + self.renew_interval:
            return None
        return node

    def leader(self) -> str | None:
        """Return the node that acts as leader, if known."""
        if self.leads:
            return self.node_id
        return self.remote_owner(LEADER_KEY)

    def owner_of_entry(self, entry_id: str) -> str | None:
        """Return the node that runs ``entry_id``."""
        entry = self.hass.config_entries.async_get_entry(entry_id)
        if entry is not None and not self.gates(entry):
            return None
        if self._is_leader:
            return self.node_id
        return self.remote_owner(LEADER_KEY)

    def standby_reason(self, entry_id: str) -> str:
        """Why an entry is not loaded on this node."""
        if self._duplicate:
            return STANDBY_DUPLICATE_NODE_ID
        if (owner := self.leader()) is not None:
            return f"Cluster standby: running on node {owner}"
        return "Cluster standby: waiting for lease"

    def add_before_setup(self, cb: Callable[[ConfigEntry], Awaitable[None]]) -> None:
        """Register a coroutine that runs before an entry sets up on promotion."""
        if cb not in self._before_setup:
            self._before_setup.append(cb)

    def add_after_demote(self, cb: Callable[[str], Awaitable[None]]) -> None:
        """Register a coroutine that runs after an entry was unloaded here."""
        if cb not in self._after_demote:
            self._after_demote.append(cb)

    def add_leader_listener(self, cb: Callable[[bool], Awaitable[None]]) -> None:
        """Register a coroutine called on leadership change."""
        if cb not in self._leader_listeners:
            self._leader_listeners.append(cb)

    def add_work_tracker(
        self,
        busy: Callable[[], bool],
        drain: Callable[[float], Awaitable[None]],
    ) -> None:
        """Register work of the leader that is not a config entry.

        ``busy`` says whether it still runs: the lease is not released cleanly
        meanwhile. ``drain(timeout)`` is awaited when leadership ends: it lets
        the work finish for ``timeout`` seconds, then stops it.
        """
        self._work_trackers.append((busy, drain))

    async def _async_drain_work(self) -> None:
        """Stop the leader's work that is not a config entry."""
        await asyncio.gather(
            *(drain(self.unload_grace) for _, drain in self._work_trackers)
        )

    def _adopt_ttl(self) -> None:
        if (ttl := self.coordinator.lease_ttl) == self.ttl:
            return
        if self.renew_interval * 2 >= ttl:
            raise HomeAssistantError(
                f"The lease bucket expires leases after {ttl}s, too soon for a"
                f" renew_interval of {self.renew_interval}s; configure the same"
                " lease_ttl on all nodes"
            )
        self.ttl = ttl
        self.fence_after = _fence_after(ttl, self.renew_interval)

    async def async_start(self, waiting: set[str]) -> None:
        """Start the leader loop and membership heartbeat."""
        self._adopt_ttl()
        await self._async_share_grace()
        self._stopping = False
        self._started_at = monotonic()
        self._observing_since = self._started_at
        self._watchdog.start()
        self._standby |= waiting
        self._watch_task = self.hass.async_create_background_task(
            self._async_watch_owners(), f"cluster {self.node_id} lease watcher"
        )
        await self._async_put_member()
        # A duplicate (or a node that is not a member yet) does not win here,
        # but its loop must run: it recovers once the other run is gone.
        leader_won = await self.async_acquire(LEADER_KEY)
        self._loop_task = self.hass.loop.create_task(
            self._async_loop(), name=f"cluster {self.node_id} lease loop"
        )
        if leader_won:
            term = self._lease_term
            self._promotion_task = self.hass.async_create_task(
                self._async_become_leader(term), f"cluster {self.node_id} leader"
            )

    async def _async_share_grace(self) -> None:
        """Publish this node's unload grace and learn the others'.

        The lease keys expire, so a holder's grace can be gone before a
        standby reads it. The storage bucket keeps it: a node that takes over
        waits for the largest grace any node ever advertised.
        """
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            return
        try:
            await kv.put(
                f"{GRACE_KEY_PREFIX}{self.node_id}", str(self.unload_grace).encode()
            )
            for key in await kv.keys():
                if not key.startswith(GRACE_KEY_PREFIX):
                    continue
                node = key.removeprefix(GRACE_KEY_PREFIX)
                with suppress(ValueError):
                    self._async_note_peer_grace(
                        node, float((await kv.get(key)).value or b"")
                    )
        except NoKeysError:
            return
        except Exception as err:  # noqa: BLE001 - the lease keys still tell
            self.coordinator.note_error(err)
            _LOGGER.debug("Could not share the unload grace: %s", err)

    @callback
    def async_enable_enforcement(self) -> None:
        """Start unloading clustered entries that run without leadership."""
        self._enforcing = True

    @callback
    def async_resign(self) -> None:
        """Stop acting as leader for good (shutdown)."""
        self._resigned = True
        self._lease_term += 1
        if self._is_leader or cluster_helper.async_is_leader(self.hass):
            self._async_notify_leader(False)

    async def async_prepare_shutdown(self) -> None:
        """Stop the work of a leader before Home Assistant stops.

        Runs before the stop event (a shutdown job), while integrations still
        expect to be unloaded. Only a node whose work all stopped releases the
        lease at the end of the shutdown; otherwise the other nodes wait for
        it to expire.
        """
        self.async_resign()
        running = [
            e
            for e in self.hass.config_entries.async_entries()
            if self._tracks(e) and e.state not in _INACTIVE_STATES
        ]
        # Together: one budget for everything that must stop.
        await asyncio.gather(
            *(self._async_unload_entry(e) for e in running),
            self._async_drain_work(),
        )
        # Work that does not stop within the unload grace keeps the lease from
        # being released (see ``_is_quiescent``); the watchdog ends it.
        await cluster_helper.async_wait_transitions(self.hass, self.unload_grace)

    async def async_stop(self, release: bool = True) -> None:
        """Stop renewing and release the leader lease and membership key.

        The leader lease is released only if all work of this node stopped.
        Otherwise the watchdog stays armed: it ends the process before
        another node can take over, if the process still runs by then.
        """
        self._stopping = True
        self._enforcing = False
        for task in (self._loop_task, self._watch_task, self._promotion_task):
            if task is not None and task is not asyncio.current_task():
                task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await task
        self._loop_task = self._watch_task = self._promotion_task = None
        if self._is_leader or cluster_helper.async_is_leader(self.hass):
            self._async_notify_leader(False)
        self._is_leader = False
        self._lease_term += 1
        cluster_helper.async_set_leader_deadline(self.hass, None)

        quiescent = self._is_quiescent()
        if release and self.coordinator.connected:
            kv = self.coordinator.kv_leases
            if TYPE_CHECKING:
                assert kv is not None
            if (lease := self._held.get(LEADER_KEY)) is not None:
                # A revision below 1 (the bucket was recreated and the lease is
                # not confirmed yet) would make nats-py delete without any
                # condition, and so remove another node's lease.
                if lease.grace_until is None and quiescent and lease.revision > 0:
                    with suppress(Exception):
                        await self._async_delete_own(kv, LEADER_KEY, lease.revision)
                elif not quiescent:
                    _LOGGER.warning(
                        "Node %s stops without releasing %s: work that runs on"
                        " the leader only did not stop. Other nodes take over"
                        " once the lease expired",
                        self.node_id,
                        LEADER_KEY,
                    )
            # A node that is not a member would delete a live node's key.
            if not self._duplicate and self._member_revision is not None:
                with suppress(Exception):
                    await self._async_delete_own(
                        kv, self.member_key, self._member_revision
                    )

        for lease in self._held.values():
            if lease.fence is not None:
                lease.fence.cancel()
        self._held.clear()
        self._handover.clear()
        if quiescent or not self._watchdog.armed:
            self._watchdog.stop()
        elif self.hass.state in (
            CoreState.stopping,
            CoreState.final_write,
            CoreState.not_running,
        ):
            # Ends the process at the deadline if it still runs by then; with
            # the exit code of the stop, not a restart.
            self._watchdog.exit_code = self.hass.exit_code

    @callback
    def async_restart_watcher(self) -> None:
        """Restart watcher after bucket recreation."""
        if self._stopping:
            return
        self._foreign.clear()
        self._member_since = None
        self._member_revision = None
        self._observing_since = monotonic()
        for lease in self._held.values():
            lease.revision = -1
        if self._watch_task is not None:
            self._watch_task.cancel()
        self._watch_task = self.hass.async_create_background_task(
            self._async_watch_owners(), f"cluster {self.node_id} lease watcher"
        )

    def _payload(self) -> bytes:
        return json_bytes(
            {
                "protocol": PROTOCOL_VERSION,
                "ha_version": HA_VERSION,
                "node": self.node_id,
                "incarnation": self.incarnation,
                "grace": self.unload_grace,
            }
        )

    @callback
    def _async_note_ha_versions(self) -> None:
        """Raise a repair issue while a live node runs another Home Assistant version.

        Only a warning: blocking would leave a cluster without a leader during
        an upgrade. Nodes that do not say their version are not counted.
        """
        now = monotonic()
        other = frozenset(
            (node, version)
            for node, version, at in self._peer_ha_versions.values()
            if node != self.node_id
            and version != HA_VERSION
            and now - at <= self.ttl + self.renew_interval
        )
        if other == self._ha_version_mismatch:
            return
        self._ha_version_mismatch = other
        if not other:
            cluster_helper.async_clear_version_mismatch(self.hass)
            return
        versions = ", ".join(
            [f"{self.node_id}: {HA_VERSION}"]
            + [f"{node}: {version}" for node, version in sorted(other)]
        )
        _LOGGER.warning(
            "Cluster nodes run different Home Assistant versions (%s)", versions
        )
        cluster_helper.async_report_version_mismatch(self.hass, versions)

    async def _async_put_member(self) -> None:
        self._async_note_ha_versions()
        if self._duplicate:
            return
        if (kv := self.coordinator.kv_leases) is None or not self.coordinator.connected:
            return
        payload = json_bytes(
            {
                **(self.member_extra() if self.member_extra is not None else {}),
                "protocol": PROTOCOL_VERSION,
                "ha_version": HA_VERSION,
                "node": self.node_id,
                "incarnation": self.incarnation,
                "since": self._member_since,
                "grace": self.unload_grace,
            }
        )
        try:
            if (known := self._member_revision) is not None:
                try:
                    self._member_revision = await kv.update(
                        self.member_key, payload, last=known
                    )
                except KeyWrongLastSequenceError:
                    if not await self._async_resolve_member_conflict(kv):
                        return
                    if self._member_revision is not None:
                        return
                    # The key expired: create it again right away.
                    await self._async_create_member(kv, payload)
            else:
                await self._async_create_member(kv, payload)
        except Exception as err:  # noqa: BLE001
            self.coordinator.note_error(err)
            _LOGGER.debug("Could not renew membership: %s", err)
            return
        if self._member_since is None and self._member_revision is not None:
            self._member_since = self._member_revision

    async def async_publish_member(self) -> None:
        """Renew the member key now (``member_extra`` changed)."""
        if not self._stopping:
            await self._async_put_member()

    def peer_member_reports(self) -> dict[str, tuple[dict[str, Any], float, bool]]:
        """Return each other node's last member key, when it was seen, and liveness.

        A node is live while its member key is renewed. The last key of a node
        that went away is kept: it says what that node ran when it was last
        heard of.
        """
        return {
            node: (
                holder,
                seen,
                self.remote_owner(f"{MEMBER_KEY_PREFIX}{node}") is not None,
            )
            for node, (holder, seen) in self._peer_members.items()
        }

    def add_before_exit(self, cb: Callable[[], None]) -> None:
        """Register a blocking call the watchdog thread makes before it ends the process."""
        if cb not in self._watchdog.before_exit:
            self._watchdog.before_exit.append(cb)

    async def _async_create_member(self, kv: KeyValue, payload: bytes) -> None:
        """Create the member key; a conflict is somebody else's key."""
        try:
            self._member_revision = await kv.create(self.member_key, payload)
        except KeyWrongLastSequenceError:
            try:
                current = await kv.get(self.member_key)
            except KeyNotFoundError:
                return  # expired meanwhile; the next tick creates it
            holder = _parse_holder(current.value)
            if holder.get("incarnation") == self.incarnation:
                self._member_revision = await kv.update(
                    self.member_key, payload, last=current.revision
                )
            elif incarnation := holder.get("incarnation"):
                # The key of an earlier run of this node_id expires within the
                # TTL. Only a key that is written again belongs to a live run.
                self._async_on_foreign_incarnation(
                    incarnation, current.revision or 0, holder.get("since")
                )

    async def _async_resolve_member_conflict(self, kv: KeyValue) -> bool:
        """Find out why a member key update conflicted.

        Returns False if the caller must stop; otherwise ``_member_revision``
        is the revision to use, or ``None`` if the key has to be created.
        """
        try:
            current = await kv.get(self.member_key)
        except KeyNotFoundError:
            self._member_revision = None
            self._member_since = None
            return True
        holder = _parse_holder(current.value)
        if holder.get("incarnation") == self.incarnation:
            # A reply was lost: the write happened.
            self._member_revision = current.revision
            return True
        self._member_revision = None
        if incarnation := holder.get("incarnation"):
            self._async_on_foreign_incarnation(
                incarnation, current.revision or 0, holder.get("since")
            )
        return False

    async def _async_previous_holder(self, key: str) -> tuple[str | None, int | None]:
        """Return the last holder of ``key``, or the revision of its release.

        ``(None, revision)`` if the last holder released the key: ``revision``
        is the delete marker it wrote. Only a write that replaces exactly that
        marker may treat the takeover as clean.
        """
        kv = self.coordinator.kv_leases
        if TYPE_CHECKING:
            assert kv is not None
        try:
            current = await kv.get(key)
        except KeyNotFoundError as err:
            if err.op == KV_DEL and err.entry is not None and err.entry.revision:
                return None, err.entry.revision
            return _UNKNOWN_HOLDER, None
        holder = _parse_holder(current.value)
        if node := holder.get("node"):
            self._async_note_peer_grace(node, holder.get("grace"))
        return holder.get("node") or _UNKNOWN_HOLDER, None

    async def async_acquire(self, key: str) -> bool:
        """Attempt to acquire a lease key."""
        if key != LEADER_KEY:
            return self.is_leader

        if LEADER_KEY in self._held:
            return True
        if self._stopping or self._duplicate or not self.coordinator.connected:
            return False
        if self._resigned or self._member_revision is None:
            # Leadership requires membership: the standbys need the member key
            # to know a leader is there, and a duplicate must be found first.
            return False
        if self._in_cooldown(LEADER_KEY):
            return False
        if (
            (seen := self._owners.get(LEADER_KEY)) is not None
            and seen[0] != self.node_id
            and monotonic() - seen[1] < self.fence_after
        ):
            return False

        if self._async_version_blocked():
            return False

        kv = self.coordinator.kv_leases
        if TYPE_CHECKING:
            assert kv is not None
        revision: int | None
        released: int | None = None
        try:
            if (previous := self.remote_owner(LEADER_KEY)) is None:
                previous, released = await self._async_previous_holder(LEADER_KEY)
            sent = self.hass.loop.time()
            if released is not None:
                # A clean release: claim exactly the delete marker that was
                # read. Any other write after it (another node's whole term,
                # or the key expiring and being created again) fails this.
                revision = await kv.update(LEADER_KEY, self._payload(), last=released)
            else:
                revision = await kv.create(LEADER_KEY, self._payload())
        except KeyWrongLastSequenceError:
            if released is not None:
                # Somebody wrote after the release; look again next time.
                return False
            sent = self.hass.loop.time()
            try:
                revision = await self._async_reclaim(LEADER_KEY)
            except _Undecided:
                return False  # look again next time
            if revision is None:
                return False
            previous = None
        except Exception as err:  # noqa: BLE001
            self.coordinator.note_error(err)
            _LOGGER.debug("Lease create for %s failed: %s", LEADER_KEY, err)
            return False

        # These change while the lease request was in flight.
        if self._stopping or self._duplicate:
            with suppress(Exception):  # type: ignore[unreachable]
                await kv.delete(LEADER_KEY, last=revision)
            return False
        self._lease_term += 1
        self._record(LEADER_KEY, revision, sent)
        self.previous_leader = (
            previous
            if previous is not None and previous not in (self.node_id, _UNKNOWN_HOLDER)
            else self._last_leader_seen
        )

        if previous is not None and previous != self.node_id:
            # The previous holder did not release the key. It may still run
            # the entries and renew until it fences itself, so no node may run
            # them before a full lease and the unload grace passed since this
            # node last saw it renew, or since it started to watch the bucket
            # (the history can be lost with the bucket, and the holder can be
            # alive behind a partition).
            seen_at = 0.0
            if (seen := self._owners.get(LEADER_KEY)) is not None and (
                seen[0] != self.node_id
            ):
                seen_at = seen[1]
            grace = self.takeover_grace
            # Loop time and monotonic time are the same instant offset.
            seen_since = max(self._observing_since, seen_at) + self.ttl
            seen_since += self.hass.loop.time() - monotonic()
            self._grace_base = (seen_since, sent)
            self._grace_until = max(self._grace_until, sent + grace, seen_since + grace)
            self._held[LEADER_KEY].grace_until = self._grace_until
            _LOGGER.info(
                "Node %s won %s after %s stopped renewing it; waiting %.1fs before running it",
                self.node_id,
                LEADER_KEY,
                "its last holder (if any)"
                if previous == _UNKNOWN_HOLDER
                else f"node {previous}",
                max(0.0, self._grace_until - self.hass.loop.time()),
            )
            self._is_handover = True
        elif previous != self.node_id:
            # This write replaced the delete marker of a clean release (the
            # previous holder stopped all its work first), so no grace is
            # needed, but the standby must adopt the latest storage data.
            self._is_handover = True
        else:
            self._is_handover = False

        self._owners[LEADER_KEY] = (self.node_id, monotonic())
        return True

    @property
    def incompatible_peers(self) -> dict[str, Any]:
        """Other nodes seen recently that write another protocol version.

        Node -> version. A node that stopped renewing without a delete (the
        server expires its keys silently) drops out after the usual window.
        """
        now = monotonic()
        return {
            node: version
            for node, version, at in self._peer_versions.values()
            if node != self.node_id
            and now - at <= self.ttl + self.renew_interval
            and not is_supported({"protocol": version})
        }

    def _async_version_blocked(self) -> bool:
        """Return True if a node of another protocol version forbids leading.

        A node does not take the lease while one is alive: the records of
        another version may mean something else, so what this node would apply
        or write could be wrong. A leader that sees such a node keeps leading
        (stepping down would leave nobody who can lead). The repair issue is
        raised at once, and removed when the node is gone or upgraded.
        """
        if not (blocking := self.incompatible_peers):
            if self._version_blocked:
                self._version_blocked = frozenset()
                _LOGGER.info(
                    "No node of another protocol version is left; node %s may"
                    " take over again",
                    self.node_id,
                )
                if self._cannot_take_over:
                    self._cannot_take_over = False
                    cluster_helper.async_clear_cannot_take_over(self.hass)
            return False
        reason = (
            "nodes of another protocol version are running ("
            + ", ".join(f"{n}: {v!r}" for n, v in sorted(blocking.items()))
            + f"), this node speaks version {PROTOCOL_VERSION}; run the same"
            " version on every node"
        )
        if (nodes := frozenset(blocking)) != self._version_blocked:
            self._version_blocked = nodes
            self._cannot_take_over = False
            _LOGGER.error("Node %s does not take over: %s", self.node_id, reason)
        if not self._cannot_take_over:
            self._cannot_take_over = True
            cluster_helper.async_report_cannot_take_over(
                self.hass, self.node_id, reason
            )
        return True

    @callback
    def _async_note_promotion_failed(self, reason: str, *, at_once: bool) -> None:
        """Count a promotion that failed; show it as a repair issue when it persists.

        ``at_once``: a handover that cannot be completed says why and does not
        go away by itself, so it is shown on the first failure.
        """
        self._promotion_failures += 1
        if at_once or self._promotion_failures >= PROMOTION_FAILURES_BEFORE_ISSUE:
            self._cannot_take_over = True
            cluster_helper.async_report_cannot_take_over(
                self.hass, self.node_id, reason
            )

    def _grace_target(self, key: str) -> float:
        """When the wait before running ``key`` ends (loop time).

        A larger grace that a peer advertises while this node waits moves it.
        """
        target = self._grace_until
        if (lease := self._held.get(key)) is not None and lease.grace_until is not None:
            target = max(target, lease.grace_until)
        if self._grace_base is not None:
            seen_since, sent = self._grace_base
            grace = self.takeover_grace
            target = max(target, sent + grace, seen_since + grace)
        return target

    async def _async_wait_grace(self, key: str) -> None:
        if self._grace_target(key) <= 0:
            return
        while (delay := self._grace_target(key) - self.hass.loop.time()) > 0:
            await asyncio.sleep(delay)
        self._grace_until = 0.0
        self._grace_base = None
        if (lease := self._held.get(key)) is not None:
            lease.grace_until = None

    @callback
    def _record(self, key: str, revision: int, sent: float) -> None:
        grace_until = None
        if (old := self._held.get(key)) is not None:
            grace_until = old.grace_until
            if old.fence is not None:
                old.fence.cancel()
        if key == LEADER_KEY and self._grace_until > self.hass.loop.time():
            grace_until = self._grace_until
        self._held[key] = _Lease(
            revision,
            sent,
            self.hass.loop.call_at(sent + self.fence_after, self._async_fence, key),
            grace_until,
        )
        if key == LEADER_KEY:
            # ``sent`` is a loop time; the same instant on the monotonic clock.
            sent_at = monotonic() - (self.hass.loop.time() - sent)
            self._fence_deadline = sent_at + self.fence_after
            self._last_own_renewal = monotonic()
            cluster_helper.async_set_leader_deadline(self.hass, self._fence_deadline)
            # The server lets a standby create the key ``ttl`` after this
            # write, and it runs the entries ``unload_grace`` later.
            self._watchdog.arm(sent_at + self.ttl + self.unload_grace - 0.3)

    @callback
    def _async_fence(self, key: str) -> None:
        if (lease := self._held.get(key)) is None:
            return
        _LOGGER.error(
            "Node %s could not renew %s for %.1fs; releasing it locally to"
            " avoid split brain",
            self.node_id,
            key,
            self.hass.loop.time() - lease.sent_at,
        )
        if key == LEADER_KEY:
            self._lease_term += 1
            self._is_leader = False
            cluster_helper.async_set_leader(self.hass, False)
        self.hass.async_create_background_task(self._async_lost(key), f"fence {key}")

    @callback
    def async_term_lost(self) -> None:
        """Another node started a newer term: stop leading at once.

        The server refused a write because the term key moved. The lease may
        still look held here (a stall, a partition): leading on would only
        produce more refused writes, and that node runs the entries now.
        """
        if LEADER_KEY not in self._held:
            return
        self._lease_term += 1
        self._is_leader = False
        cluster_helper.async_set_leader(self.hass, False)
        self.hass.async_create_background_task(
            self._async_lost(LEADER_KEY), f"cluster {self.node_id} term lost"
        )

    def _may_reclaim_previous_run(self) -> bool:
        if self._duplicate or monotonic() < self._conflict_until:
            return False
        return monotonic() - self._started_at >= self._reclaim_after

    async def _async_reclaim(self, key: str) -> int | None:
        """Find out whether this node still holds ``key`` after a refused renewal.

        Returns the new revision if it does, None if another node holds it.
        Raises ``_Undecided`` if the answer cannot be had now (a read or write
        failed): that is not "lost". The caller tries again at the next tick;
        the fence still ends leadership at its deadline if it keeps failing.
        """
        kv = self.coordinator.kv_leases
        if TYPE_CHECKING:
            assert kv is not None
        try:
            current = await kv.get(key)
        except KeyNotFoundError:
            return await self._async_recreate_vanished(key)
        except Exception as err:
            self.coordinator.note_error(err)
            raise _Undecided from err
        holder = _parse_holder(current.value)
        node = holder.get("node", _UNKNOWN_HOLDER)
        self._owners[key] = (node, monotonic())
        if node != self.node_id:
            return None
        same_run = holder.get("incarnation") == self.incarnation
        if not same_run and not self._may_reclaim_previous_run():
            return None
        try:
            revision = await kv.update(key, self._payload(), last=current.revision)
        except Exception as err:
            self.coordinator.note_error(err)
            if self.coordinator.is_refused_write(err):
                return None  # written by someone else since the read
            raise _Undecided from err
        if same_run:
            _LOGGER.info(
                "Node %s resynced %s after a write whose reply was lost",
                self.node_id,
                key,
            )
        else:
            _LOGGER.info(
                "Node %s reclaimed %s from its previous run", self.node_id, key
            )
        return revision

    async def _async_recreate_vanished(self, key: str) -> int | None:
        if key not in self._held:
            return None
        kv = self.coordinator.kv_leases
        if TYPE_CHECKING:
            assert kv is not None
        try:
            revision = await kv.create(key, self._payload())
        except Exception as err:
            self.coordinator.note_error(err)
            if self.coordinator.is_refused_write(err):
                return None  # somebody created it first
            raise _Undecided from err
        _LOGGER.warning(
            "Node %s created %s again after it vanished from NATS", self.node_id, key
        )
        return revision

    def _in_cooldown(self, key: str) -> bool:
        if self._cooldown_until <= 0:
            return False
        if monotonic() >= self._cooldown_until or not self.members:
            self._cooldown_until = 0.0
            return False
        return True

    async def async_place_entry(self, entry: ConfigEntry) -> str | None:
        """Check placement for an entry without blocking network I/O."""
        if not self.gates(entry):
            return None
        if self._duplicate:
            return STANDBY_DUPLICATE_NODE_ID
        if self._resigned:
            return STANDBY_SHUTTING_DOWN
        # A leader runs its entries even while NATS is away; ``is_leader``
        # expires by itself during a long outage. Anything else is retried
        # when the node leads, so it must be remembered.
        if self.is_leader:
            return None
        self._standby.add(entry.entry_id)
        return self.standby_reason(entry.entry_id)

    async def _async_run_before_setup(self, entry: ConfigEntry) -> bool:
        for cb in self._before_setup:
            try:
                await cb(entry)
            except HandoverIncomplete as err:
                _LOGGER.warning(
                    "Node %s not starting %s (%s) yet: %s",
                    self.node_id,
                    entry.title,
                    entry.domain,
                    err,
                )
                return False
            except Exception:
                # Fail closed: a prerequisite that failed unexpectedly (for
                # example the credential check) is not a reason to start the
                # entry. It stays on standby and is tried again.
                if entry.entry_id not in self._setup_blocked:
                    _LOGGER.exception(
                        "Pre-setup hook failed for %s; not starting it", entry.entry_id
                    )
                self._setup_blocked.add(entry.entry_id)
                if entry.state is ConfigEntryState.NOT_LOADED:
                    self.hass.config_entries._async_set_standby(  # noqa: SLF001
                        entry.entry_id, STANDBY_SETUP_BLOCKED
                    )
                return False
        self._setup_blocked.discard(entry.entry_id)
        return True

    async def _async_release(self, key: str) -> None:
        """Stop holding ``key``; delete it only if all its work stopped here.

        A deleted key lets another node take over at once. If work may still
        run here (an unload failed or is still in progress), the key is left
        to expire instead: other nodes then wait for the lease and the unload
        grace, and the watchdog ends this process before that.
        """
        if (lease := self._held.pop(key, None)) is None:
            return
        if lease.fence is not None:
            lease.fence.cancel()
        quiescent = self._is_quiescent()
        self._async_disarm_watchdog()
        if lease.grace_until is not None:
            return
        if not quiescent:
            _LOGGER.error(
                "Node %s gives up %s while work that runs on the leader only"
                " has not stopped; other nodes take over once it expired",
                self.node_id,
                key,
            )
            return
        if lease.revision < 1:
            return  # not confirmed since the bucket was recreated: let it expire
        if (
            kv := self.coordinator.kv_leases
        ) is not None and self.coordinator.connected:
            try:
                await self._async_delete_own(kv, key, lease.revision)
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Could not release %s: %s", key, err)

    async def _async_delete_own(self, kv: KeyValue, key: str, revision: int) -> None:
        """Delete ``key`` at ``revision``, or at its current one if it is still ours.

        ``revision`` can be one behind: a renewal that was in flight when the
        lease loop was cancelled may have been stored after all. The key is
        then read once and deleted at its current revision, but only if this
        incarnation of the node wrote it; another node's key is never deleted.
        """
        try:
            await kv.delete(key, last=revision)
        except Exception as err:
            # nats-py raises a plain ``APIError`` for a refused delete.
            if not ClusterCoordinator.is_refused_write(err):
                raise
            try:
                current = await kv.get(key)
            except KeyNotFoundError:
                return  # gone already (expired, or deleted)
            if (
                current.revision is None
                or _parse_holder(current.value).get("incarnation") != self.incarnation
            ):
                raise
            _LOGGER.debug(
                "Releasing %s at revision %s: a renewal was stored after all",
                key,
                current.revision,
            )
            await kv.delete(key, last=current.revision)

    async def _async_loop(self) -> None:
        while not self._stopping:
            started = monotonic()
            try:
                await self._async_tick()
            except Exception:
                _LOGGER.exception("Lease loop iteration failed")
            await asyncio.sleep(
                max(0.05, self.renew_interval - (monotonic() - started))
            )

    async def _async_tick(self) -> None:
        if self._held:
            await asyncio.gather(*(self._async_renew(k) for k in list(self._held)))
        await self._async_put_member()

        if self._duplicate and monotonic() >= self._conflict_until:
            await self._async_recover_duplicate()

        if (
            not self.is_leader
            and not self._resigned
            and not self._duplicate
            and LEADER_KEY not in self._held
        ):
            async with self._election_lock:
                if (
                    not self.is_leader
                    and not self._resigned
                    and not self._duplicate
                    and LEADER_KEY not in self._held
                    and await self.async_acquire(LEADER_KEY)
                ):
                    term = self._lease_term
                    self._promotion_task = self.hass.async_create_background_task(
                        self._async_become_leader(term),
                        f"cluster {self.node_id} leader",
                    )

        self._async_enforce()

    async def _async_recover_duplicate(self) -> None:
        """Take part again once the other run of this node_id is gone."""
        kv = self.coordinator.kv_leases
        if kv is None or not self.coordinator.connected:
            return
        try:
            current = await kv.get(self.member_key)
        except KeyNotFoundError:
            holder: dict[str, Any] = {}
        except Exception as err:  # noqa: BLE001
            self.coordinator.note_error(err)
            _LOGGER.debug("Could not check the member key: %s", err)
            return
        else:
            holder = _parse_holder(current.value)
        if holder and holder.get("incarnation") != self.incarnation:
            return
        self._duplicate = False
        self._member_revision = None
        self._member_since = None
        self._foreign.clear()
        await self._async_put_member()

    @callback
    def _async_enforce(self) -> None:
        """Enforce single-leader invariants."""
        if not self._enforcing:
            return

        if not self.is_leader:
            for entry in self.hass.config_entries.async_entries():
                if not self._tracks(entry):
                    continue
                if (
                    entry.state not in _INACTIVE_STATES
                    and entry.entry_id not in self._demoting
                    and self.gates(entry)
                ):
                    _LOGGER.warning(
                        "%s (%s) is active on node %s without leadership; unloading it",
                        entry.title,
                        entry.domain,
                        self.node_id,
                    )
                    self.hass.async_create_background_task(
                        self.async_demote(entry.entry_id), f"demote {entry.entry_id}"
                    )
                if entry.state not in _INACTIVE_STATES and not self._watchdog.armed:
                    self._watchdog.arm(
                        monotonic() + (self.ttl - self.fence_after) + self.unload_grace
                    )
            return

        if self._resigned:
            # Stopping: nothing starts here any more.
            return

        # On the leader: promote entries waiting on standby
        for entry_id in list(self._standby):
            standby_entry = self.hass.config_entries.async_get_entry(entry_id)
            if standby_entry is None or not self.gates(standby_entry):
                self._standby.discard(entry_id)
                continue
            if standby_entry.state is ConfigEntryState.NOT_LOADED:
                self._standby.discard(entry_id)
                self.hass.async_create_background_task(
                    self._async_setup_promoted_entry(standby_entry), f"setup {entry_id}"
                )

        # Health handoff: if all gated entries fail continuously, hand off leadership
        gated = [e for e in self.hass.config_entries.async_entries() if self.gates(e)]
        if not gated:
            self._failing_since = None
            return

        any_loaded = any(e.state is ConfigEntryState.LOADED for e in gated)
        if any_loaded:
            self._failing_since = None
        else:

            def _failing(e: ConfigEntry) -> bool:
                # An entry whose pre-setup step fails runs nowhere: failing too.
                return e.state in _FAILING_STATES or e.entry_id in self._setup_blocked

            all_failing_or_retrying = all(
                _failing(e) or e.state is ConfigEntryState.SETUP_IN_PROGRESS
                for e in gated
            )
            if all_failing_or_retrying:
                now = monotonic()
                if self._failing_since is None and any(_failing(e) for e in gated):
                    self._failing_since = now
                handoff_after = max(self.ttl * 2, FAILED_SETUP_HANDOFF_AFTER)
                if (
                    self._failing_since is not None
                    and now - self._failing_since >= handoff_after
                    and self.members
                    and not self._handing_off
                ):
                    _LOGGER.warning(
                        "All gated entries failed on node %s for %.1fs; handing leadership to another node",
                        self.node_id,
                        now - self._failing_since,
                    )
                    self.hass.async_create_background_task(
                        self._async_hand_off_leader(), f"hand off leader {self.node_id}"
                    )

    async def _async_setup_promoted_entry(self, entry: ConfigEntry) -> None:
        if self._resigned or not self.is_leader:
            self._standby.add(entry.entry_id)
            return
        self._promoted_setups.add(entry.entry_id)
        try:
            if not await self._async_run_before_setup(entry):
                self._standby.add(entry.entry_id)
                return
            try:
                await self.hass.config_entries.async_setup(entry.entry_id)
            except OperationNotAllowed as err:
                _LOGGER.debug("Setup of %s skipped: %s", entry.entry_id, err)
            except Exception:
                _LOGGER.exception("Failed setup of %s during promotion", entry.entry_id)
            self._handover.discard(entry.entry_id)
        finally:
            self._promoted_setups.discard(entry.entry_id)

    def _is_term_valid(self, term: int) -> bool:
        """Check if lease term is still valid for leadership and promotion."""
        return (
            self._lease_term == term
            and LEADER_KEY in self._held
            and not self._resigned
            and not self._duplicate
            and monotonic() < self._fence_deadline
        )

    async def _async_become_leader(self, term: int | None = None) -> None:
        if term is None:
            term = self._lease_term
        await self._async_wait_grace(LEADER_KEY)
        async with self._election_lock:
            if not self._is_term_valid(term) or self._is_leader:
                return
            # The check when the lease was taken is older than the grace wait:
            # a node of another protocol version may have appeared since.
            if self._async_version_blocked():
                await self._async_refuse_promotion()
                return
            await self._async_promote_leader(term)

    async def _async_refuse_promotion(self) -> None:
        """Give the lease back before leading: a node of another version is alive."""
        _LOGGER.warning(
            "Node %s gives up the lease it took: a node of another protocol"
            " version appeared before it could lead",
            self.node_id,
        )
        self._lease_term += 1
        self._cooldown_until = monotonic() + max(self.ttl * 2, HANDOFF_COOLDOWN)
        await self._async_release(LEADER_KEY)

    async def _async_promote_leader(self, term: int | None = None) -> None:
        if term is None:
            term = self._lease_term
        if not self._is_term_valid(term):
            return
        try:
            await self._async_run_promotion(term)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            # Whatever failed, the lease must not stay held and renewed by a
            # node that does not lead: nobody could take over. Undo what the
            # promotion did, give the lease up and let another node try.
            _LOGGER.exception(
                "Promotion of node %s failed; giving up the leadership", self.node_id
            )
            self._async_note_promotion_failed(
                f"the promotion raised {err!r}", at_once=False
            )
            try:
                await self._async_abort_promotion()
            except asyncio.CancelledError:
                raise
            except Exception:
                # The lease was given up anyway (see ``_async_abort_promotion``);
                # what failed is in the log, not in a task nobody awaits.
                _LOGGER.exception("Undoing the promotion of %s failed", self.node_id)

    async def _async_abort_promotion(self) -> None:
        """Undo a promotion that raised: unload, release, back off."""
        if self._handing_off:
            return
        self._handing_off = True
        try:
            if self.replicator is not None:
                self.replicator.async_resume_mirroring()
            self._lease_term += 1
            self._cooldown_until = monotonic() + max(self.ttl * 2, HANDOFF_COOLDOWN)
            # Entries may be running already: unload them like a demotion does
            # (it also waits for the work that stopping them started). The
            # lease is given up whatever happens: a renewal must not resume.
            try:
                await self._async_lose_leader()
            finally:
                await self._async_release(LEADER_KEY)
        finally:
            self._handing_off = False

    async def _async_run_promotion(self, term: int) -> None:
        _LOGGER.info("Node %s is now cluster leader", self.node_id)
        if self.replicator is not None:
            await self.replicator.async_before_promote()
            if not self._is_term_valid(term):
                self.replicator.async_resume_mirroring()
                return

        if self.storage is not None:
            try:
                await self.storage.async_before_promote(self._is_handover)
            except HandoverIncomplete as err:
                _LOGGER.error(
                    "Promotion handover failed: %s; releasing leadership", err
                )
                self._async_note_promotion_failed(str(err), at_once=True)
                if self.replicator is not None:
                    self.replicator.async_resume_mirroring()
                self._lease_term += 1
                self._cooldown_until = monotonic() + max(self.ttl * 2, HANDOFF_COOLDOWN)
                await self._async_release(LEADER_KEY)
                return

        if not self._is_term_valid(term):
            if self.replicator is not None:
                self.replicator.async_resume_mirroring()
            return

        # The handover can take a while: look again before this node leads.
        if self._async_version_blocked():
            if self.replicator is not None:
                self.replicator.async_resume_mirroring()
            await self._async_refuse_promotion()
            return

        if self._is_handover:
            self._handover.update(
                e.entry_id
                for e in self.hass.config_entries.async_entries()
                if self.gates(e)
            )

        self._is_leader = True
        self._failing_since = None
        self._async_notify_leader(True)

        for entry in self.hass.config_entries.async_entries():
            if not self._is_term_valid(term):
                _LOGGER.warning("Leadership lost during entry promotion")
                self._is_leader = False
                cluster_helper.async_set_leader(self.hass, False)
                self._async_enforce()
                return
            if (
                self.gates(entry)
                and entry.disabled_by is None
                and entry.state is ConfigEntryState.NOT_LOADED
            ):
                self._standby.discard(entry.entry_id)
                await self._async_setup_promoted_entry(entry)

        if not self._is_term_valid(term):
            self._is_leader = False
            cluster_helper.async_set_leader(self.hass, False)
            self._async_enforce()
            return

        self._served_term = term
        self._is_handover = False
        self._promotion_failures = 0
        if self._cannot_take_over:
            self._cannot_take_over = False
            cluster_helper.async_clear_cannot_take_over(self.hass)

        if self.replicator is not None:
            self.replicator.publish_owned()
        if self.storage is not None:
            try:
                await self.storage.async_flush()
            except HomeAssistantError as err:
                # A failed flush must not abort the promotion; the storage
                # mirror keeps retrying what is still queued.
                _LOGGER.warning("Could not publish storage after promotion: %s", err)

    async def _async_renew(self, key: str) -> None:
        if (lease := self._held.get(key)) is None:
            return
        if self._handing_off:
            # Giving up the lease: renewing it could keep it, and the watchdog
            # that guards the unload, alive for as long as the unload takes.
            return
        kv = self.coordinator.kv_leases
        if TYPE_CHECKING:
            assert kv is not None
        sent = self.hass.loop.time()
        revision: int | None
        try:
            revision = await kv.update(key, self._payload(), last=lease.revision)
        except KeyWrongLastSequenceError:
            sent = self.hass.loop.time()
            try:
                revision = await self._async_reclaim(key)
            except _Undecided:
                return  # try again at the next tick; the fence still bounds us
            if revision is None:
                _LOGGER.error("Node %s lost lease %s", self.node_id, key)
                await self._async_lost(key)
                return
        except Exception as err:  # noqa: BLE001
            self.coordinator.note_error(err)
            _LOGGER.debug("Renewal of %s failed: %s", key, err)
            return
        if key in self._held and not self._stopping:
            self._record(key, revision, sent)

    async def _async_lost(self, key: str) -> None:
        if (lease := self._held.pop(key, None)) is None:
            return
        if lease.fence is not None:
            lease.fence.cancel()
        if key == LEADER_KEY:
            self._lease_term += 1
            self._is_leader = False
            cluster_helper.async_set_leader(self.hass, False)
            await self._async_lose_leader()

    async def _async_lose_leader(self) -> None:
        """Demote this node from leadership and unload all gated entries.

        Returns once the entries are unloaded and the work the change of
        leadership started (stopping automations, scripts and YAML
        platforms) is done; the watchdog stays armed until then.
        """
        self._is_leader = False
        self._failing_since = None
        self.coordinator.async_end_term()
        self._async_notify_leader(False)

        running = [
            e
            for e in self.hass.config_entries.async_entries()
            if self._tracks(e) and e.state not in _INACTIVE_STATES
        ]
        await asyncio.gather(
            *(self._async_unload_entry(e) for e in running),
            self._async_drain_work(),
        )
        await cluster_helper.async_wait_transitions(self.hass)

        if self.replicator is not None:
            await self.replicator.async_after_demote()
        self._async_disarm_watchdog()

    @callback
    def _async_disarm_watchdog(self) -> None:
        """Disarm the watchdog once nothing needs to be stopped any more."""
        if LEADER_KEY in self._held or not self._is_quiescent():
            return
        self._watchdog.disarm()

    async def async_demote(self, entry_id: str) -> None:
        """Stop running an entry here and put it on standby."""
        if (entry := self.hass.config_entries.async_get_entry(entry_id)) is None:
            return
        await self._async_unload_entry(entry)

    async def _async_unload_entry(self, entry: ConfigEntry) -> None:
        key = entry.entry_id
        if key in self._demoting:
            return
        self._demoting.add(key)
        try:
            async with entry.setup_lock:
                if entry.state not in _INACTIVE_STATES:
                    try:
                        await self.hass.config_entries.async_unload(key, _lock=False)
                    except OperationNotAllowed as err:
                        _LOGGER.warning("Could not unload %s: %s", key, err)
                if entry.state is ConfigEntryState.NOT_LOADED:
                    self._standby.add(key)
                    self.hass.config_entries._async_set_standby(  # noqa: SLF001
                        key, self.standby_reason(key)
                    )
                elif entry.state in _RUNNING_STATES:
                    self._async_failed_to_stop(entry)
            await self._async_run_after_demote(key)
        except Exception:
            _LOGGER.exception("Failed to unload %s during demotion", key)
            self._async_failed_to_stop(entry)
        finally:
            self._demoting.discard(key)
            self._async_disarm_watchdog()

    async def _async_hand_off_leader(self) -> None:
        """Step down from leadership due to persistent entry failures."""
        if self._handing_off:
            return
        self._handing_off = True
        try:
            self._cooldown_until = monotonic() + max(self.ttl * 2, HANDOFF_COOLDOWN)
            await self._async_lose_leader()
            await self._async_release(LEADER_KEY)
        finally:
            self._handing_off = False

    @callback
    def _async_failed_to_stop(self, entry: ConfigEntry) -> None:
        if self._fatal:
            return
        self._fatal = True
        if self._resigned or self._stopping:
            # Home Assistant is stopping already. The lease is not released,
            # so other nodes wait for it to expire.
            _LOGGER.error(
                "Node %s could not unload %s (%s: %s) while stopping",
                self.node_id,
                entry.title,
                entry.state,
                entry.reason,
            )
            return
        _LOGGER.critical(
            "Node %s lost leadership but could not unload %s (%s: %s);"
            " restarting Home Assistant to prevent split brain",
            self.node_id,
            entry.title,
            entry.state,
            entry.reason,
        )
        # The watchdog stays armed (the entry still runs) and ends the process
        # at its deadline if the restart has not.
        self.async_restart()

    @callback
    def async_restart(self) -> None:
        """Restart Home Assistant."""
        if (handler := self.hass.data.get(DATA_STOP_HANDLER)) is not None:
            coro = handler(self.hass, True)
        else:
            coro = self.hass.async_stop(RESTART_EXIT_CODE)
        self._restart_task = asyncio.create_task(coro)

    async def _async_stand_down(self) -> None:
        """Another instance has this node id: give up everything held."""
        self._lease_term += 1
        if self._promotion_task is not None and not self._promotion_task.done():
            self._promotion_task.cancel()
        if self._is_leader:
            await self._async_lose_leader()
        await self._async_release(LEADER_KEY)
        self._async_disarm_watchdog()

    @callback
    def _async_on_foreign_incarnation(
        self, incarnation: str, revision: int, since: Any
    ) -> None:
        if self._foreign.setdefault(incarnation, revision) >= revision:
            return
        now = monotonic()
        new_conflict = now >= self._conflict_until
        self._conflict_until = now + 3 * self.ttl
        if self._duplicate:
            return
        theirs = since if isinstance(since, int) else revision
        if self._member_since is None or theirs < self._member_since:
            self._duplicate = True
            self._lease_term += 1
            if self._promotion_task is not None and not self._promotion_task.done():
                self._promotion_task.cancel()
            _LOGGER.error(
                "Another Home Assistant instance runs with node_id %s; this"
                " instance started later and stays on standby. Give every"
                " cluster node its own node_id",
                self.node_id,
            )
            self.hass.async_create_background_task(
                self._async_stand_down(), f"cluster {self.node_id} stand down"
            )
        elif new_conflict:
            _LOGGER.error(
                "Another Home Assistant instance started with node_id %s; it"
                " stays on standby. Give every cluster node its own node_id",
                self.node_id,
            )

    async def _async_run_after_demote(self, entry_id: str) -> None:
        for cb in self._after_demote:
            try:
                await cb(entry_id)
            except Exception:
                _LOGGER.exception("After-demote hook failed for %s", entry_id)

    @callback
    def _async_notify_leader(self, leader: bool) -> None:
        cluster_helper.async_set_leader(self.hass, leader)
        previous = self._leader_notify
        self._leader_notify = self.hass.async_create_background_task(
            self._async_run_leader_listeners(leader, previous),
            f"cluster {self.node_id} leader listeners ({leader})",
        )

    async def _async_run_leader_listeners(
        self, leader: bool, previous: asyncio.Task[None] | None
    ) -> None:
        if previous is not None and not previous.done():
            await asyncio.wait([previous])
        for cb in self._leader_listeners:
            try:
                await cb(leader)
            except Exception:
                _LOGGER.exception("Leader listener failed")

    async def _async_watch_owners(self) -> None:
        while not self._stopping:
            try:
                kv = self.coordinator.kv_leases
                if TYPE_CHECKING:
                    assert kv is not None
                watcher = await kv.watchall()
                try:
                    async for update in watcher:
                        if update is None:
                            continue
                        if update.operation == "DEL":
                            self._peer_versions.pop(update.key, None)
                            self._peer_ha_versions.pop(update.key, None)
                            if (seen := self._owners.pop(update.key, None)) is not None:
                                self._peer_incarnations.pop(seen[0], None)
                            if (
                                update.key == LEADER_KEY
                                and not self.is_leader
                                and not self._resigned
                                and not self._duplicate
                            ):
                                self.hass.async_create_background_task(
                                    self._async_try_elect_on_release(),
                                    f"cluster {self.node_id} elect on release",
                                )
                            continue
                        if update.operation == "PURGE":
                            self._peer_versions.pop(update.key, None)
                            self._peer_ha_versions.pop(update.key, None)
                            continue
                        holder = _parse_holder(update.value)
                        if not (node := holder.get("node")):
                            continue
                        self._peer_versions[update.key] = (
                            node,
                            version_of(holder),
                            monotonic(),
                        )
                        if isinstance(ha_version := holder.get("ha_version"), str):
                            self._peer_ha_versions[update.key] = (
                                node,
                                ha_version,
                                monotonic(),
                            )
                        self._async_note_ha_versions()
                        self._owners[update.key] = (node, monotonic())
                        if (
                            update.key.startswith(MEMBER_KEY_PREFIX)
                            and node != self.node_id
                        ):
                            self._peer_members[node] = (holder, monotonic())
                        if update.key == LEADER_KEY and node != self.node_id:
                            self._last_leader_seen = node
                        if inc := holder.get("incarnation"):
                            self._peer_incarnations[node] = inc
                        self._async_note_peer_grace(node, holder.get("grace"))
                        if (
                            update.key == self.member_key
                            and (incarnation := holder.get("incarnation"))
                            and incarnation != self.incarnation
                        ):
                            self._async_on_foreign_incarnation(
                                incarnation, update.revision, holder.get("since")
                            )
                finally:
                    await stop_watcher(watcher)
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                self.coordinator.note_error(err)
                _LOGGER.debug("Lease watcher restarting: %s", err)
            await asyncio.sleep(1)

    async def _async_try_elect_on_release(self) -> None:
        async with self._election_lock:
            if (
                not self.is_leader
                and not self._resigned
                and not self._duplicate
                and LEADER_KEY not in self._held
                and await self.async_acquire(LEADER_KEY)
            ):
                term = self._lease_term
                self._promotion_task = self.hass.async_create_background_task(
                    self._async_become_leader(term), f"cluster {self.node_id} leader"
                )


def _parse_holder(value: bytes | None) -> dict[str, Any]:
    try:
        data = json_loads(value or b"{}")
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}
