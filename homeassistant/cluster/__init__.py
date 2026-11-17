"""Run one Home Assistant configuration on several nodes (a cluster).

Every node runs Home Assistant with the same configuration and this
package. One node is the leader (a single lease in NATS JetStream) and runs the
config entries of the clustered domains, automations and scripts; the other
nodes show the entities by mirroring their states and forward service calls to
the leader. When the leader fails, another node takes over within seconds.

Core hooks used:

* ``ConfigEntries.async_set_placement``: decides where each entry runs.
* ``ServiceRegistry.async_set_router``: routes service calls to the owner.
* ``storage.async_track_store_writes``: replicates storage.
* ``cluster.async_set_leader`` (helper): gates automations and scripts.
* Internal methods of Core objects, called from this package only:
  ``ConfigEntries._async_remove_replica``, ``ConfigEntries._async_set_standby``
  and ``ServiceRegistry._async_call_local``.

Bootstrap sets this package up before any config entry. Until NATS is
reachable, clustered entries stay on standby (fail closed).

Shutdown: when Home Assistant stops, the node stops acting as leader at once
(automations detach). The leases are renewed until the close stage, after
the integrations stopped and the final save ran; the node then publishes the
config entries it holds and releases the leases, so another node takes over
without waiting for them to expire.
"""

import asyncio
import ipaddress
import logging
from typing import Any
from urllib.parse import urlsplit

import probatio

from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.const import EVENT_HOMEASSISTANT_CLOSE, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import CALLBACK_TYPE, Event, HassJob, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import cluster as cluster_helper, config_validation as cv
from homeassistant.helpers.typing import ConfigType
from homeassistant.setup import async_when_setup
from homeassistant.util.hass_dict import HassKey

from .addon_gate import SupervisorAddonGate, managed_addon_list
from .addon_snapshot import SnapshotStore, check_relative_path
from .const import (
    ADDON_SLUG_RE,
    BUCKET_STORAGE,
    CONF_ADDON_BACKUP_LOCATION,
    CONF_ADDON_SEED_NODE,
    CONF_ADDON_SNAPSHOT_INTERVAL,
    CONF_ADDON_TAKEOVER_WAIT,
    CONF_ENCRYPTION_KEY,
    CONF_EXCLUDE,
    CONF_EXCLUDE_DOMAINS,
    CONF_LEASE_TTL,
    CONF_LOCAL_ADDONS,
    CONF_MANAGED_ADDONS,
    CONF_NATS_CA,
    CONF_NATS_CREDS,
    CONF_NATS_PASSWORD,
    CONF_NATS_TOKEN,
    CONF_NATS_URL,
    CONF_NATS_USER,
    CONF_NODE_ID,
    CONF_PATHS,
    CONF_RENEW_INTERVAL,
    CONF_REPLICAS,
    CONF_REPLICATE,
    CONF_RPC_TIMEOUT,
    CONF_SECURITY,
    CONF_SEED_NODE,
    CONF_SKIP_ADDONS,
    CONF_SLUG,
    CONF_STORAGE_EXCLUDE,
    CONF_UNLOAD_GRACE,
    DEFAULT_ADDON_SNAPSHOT_INTERVAL,
    DEFAULT_ADDON_TAKEOVER_WAIT,
    DEFAULT_LEASE_TTL,
    DEFAULT_LOCAL_ADDONS,
    DEFAULT_NATS_URL,
    DEFAULT_RENEW_INTERVAL,
    DEFAULT_REPLICAS,
    DEFAULT_RPC_TIMEOUT,
    DEFAULT_SECURITY,
    DEFAULT_UNLOAD_GRACE,
    DOMAIN,
    INITIAL_CONNECT_TIMEOUT,
    NODE_ID_RE,
    SECURITY_PERMISSIVE,
    SECURITY_STRICT,
    STANDBY_WAITING_FOR_COORDINATOR,
)
from .coordinator import ClusterCoordinator, ConnectionRefused
from .crypto import MIN_KEY_LENGTH, BlobCipher, RecordCipher, backup_password
from .event_replicator import EventReplicator
from .health import async_register_leader_view
from .lease_manager import LeaseManager
from .service_router import ServiceRouter
from .state_replicator import StateReplicator
from .storage_sync import StorageSync

_LOGGER = logging.getLogger(__name__)

DATA_NODE: HassKey[ClusterNode] = HassKey(DOMAIN)

_ACTIVE_STATES = (
    ConfigEntryState.LOADED,
    ConfigEntryState.SETUP_RETRY,
    ConfigEntryState.SETUP_IN_PROGRESS,
)


def _check_timing(conf: dict[str, Any]) -> dict[str, Any]:
    if conf[CONF_RENEW_INTERVAL] * 2 >= conf[CONF_LEASE_TTL]:
        raise probatio.Invalid(
            f"{CONF_RENEW_INTERVAL} must be less than half of {CONF_LEASE_TTL}"
        )
    return conf


# How often a leader tries to start a term again before it stops leading.
_TERM_RESTART_ATTEMPTS = 10


def _is_loopback(server: str) -> bool:
    """Return True if a NATS server URL names this host.

    ``localhost`` or an address of the loopback range (``127.0.0.0/8``, ``::1``,
    and the IPv4-mapped form of those). Nothing is resolved: a name that is not
    ``localhost`` is not trusted to stay on this host.
    """
    host = (urlsplit(server.strip()).hostname or "").lower()
    if host == "localhost":
        return True
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return False
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    return address.is_loopback


def _check_security(conf: dict[str, Any]) -> dict[str, Any]:
    """In ``strict`` mode, require TLS (or loopback) and credentials.

    Every node that can publish to the cluster's subjects can call services
    and write its records, so the connection must be authenticated and, off
    this host, encrypted. ``security: permissive`` allows anything (tests, a
    trusted network).
    """
    if conf[CONF_SECURITY] != SECURITY_STRICT:
        return conf
    # A shared token cannot tell the nodes apart: the permissions that keep
    # one node from calling services as another need a user of its own.
    if not (
        conf.get(CONF_NATS_CREDS)
        or (conf.get(CONF_NATS_USER) and conf.get(CONF_NATS_PASSWORD))
    ):
        raise probatio.Invalid(
            "security is strict: configure NATS credentials of this node's own"
            f" ({CONF_NATS_CREDS}, or {CONF_NATS_USER} and {CONF_NATS_PASSWORD};"
            f" a shared {CONF_NATS_TOKEN} does not tell the nodes apart), or set"
            f" {CONF_SECURITY}: {SECURITY_PERMISSIVE}"
        )
    if not conf.get(CONF_ENCRYPTION_KEY):
        raise probatio.Invalid(
            "security is strict: configure encryption_key, the key that encrypts"
            " config entry records (tokens, API keys) before they reach the NATS"
            " server; the same on every node. Make one with"
            ' `python3 -c "import secrets; print(secrets.token_urlsafe(32))"`'
            f" and keep it in secrets.yaml, or set {CONF_SECURITY}:"
            f" {SECURITY_PERMISSIVE}"
        )
    for server in conf[CONF_NATS_URL].split(","):
        parts = urlsplit(server.strip())
        if parts.scheme != "tls" and not _is_loopback(server):
            # Host and port only: the URL can carry a user and a password, and
            # this message goes to the log and to the repair issue.
            try:
                where = (
                    f"{parts.hostname}:{parts.port}" if parts.port else parts.hostname
                )
            except ValueError:
                where = parts.hostname
            raise probatio.Invalid(
                f"security is strict: the connection to {where or 'the server'} is"
                f" not encrypted; use a tls:// URL, or set {CONF_SECURITY}:"
                f" {SECURITY_PERMISSIVE}"
            )
    return conf


def _relative_path(value: Any) -> str:
    try:
        return check_relative_path(cv.string(value))
    except ValueError as err:
        raise probatio.Invalid(str(err)) from err


_ADDON_SLUG = probatio.All(cv.string, probatio.Match(ADDON_SLUG_RE))
_MANAGED_ADDON = probatio.Any(
    _ADDON_SLUG,
    probatio.Schema(
        {
            probatio.Required(CONF_SLUG): _ADDON_SLUG,
            # Without it: the folders the add-on's options name.
            probatio.Optional(CONF_PATHS): probatio.All(
                probatio.EnsureList(), [_relative_path]
            ),
            probatio.Optional(CONF_EXCLUDE, default=[]): probatio.All(
                probatio.EnsureList(), [cv.string]
            ),
            probatio.Optional(CONF_REPLICATE, default=True): cv.boolean,
            probatio.Optional(CONF_SEED_NODE): probatio.All(
                cv.string, probatio.Match(NODE_ID_RE)
            ),
        }
    ),
)


def _check_addons(conf: dict[str, Any]) -> dict[str, Any]:
    """Snapshots of add-ons hold secrets (a Zigbee network key): require the key."""
    named, manage_all = managed_addon_list(conf[CONF_MANAGED_ADDONS])
    replicated = [addon.slug for addon in named if addon.replicate]
    if manage_all:
        replicated.insert(0, "all")
    if replicated and not conf.get(CONF_ENCRYPTION_KEY):
        raise probatio.Invalid(
            f"{CONF_MANAGED_ADDONS} {', '.join(replicated)} are snapshotted to NATS"
            " encrypted: configure encryption_key, or set replicate: false for an"
            " add-on that may start with whatever data a node has"
        )
    return conf


CONFIG_SCHEMA = probatio.Schema(
    {
        DOMAIN: probatio.All(
            probatio.Schema(
                {
                    probatio.Required(CONF_NODE_ID): probatio.All(
                        cv.string, probatio.Match(NODE_ID_RE)
                    ),
                    probatio.Optional(
                        CONF_NATS_URL, default=DEFAULT_NATS_URL
                    ): cv.string,
                    probatio.Optional(CONF_NATS_USER): cv.string,
                    probatio.Optional(CONF_NATS_PASSWORD): cv.string,
                    probatio.Optional(CONF_NATS_TOKEN): cv.string,
                    probatio.Optional(CONF_NATS_CREDS): cv.isfile,
                    probatio.Optional(CONF_NATS_CA): cv.isfile,
                    probatio.Optional(CONF_ENCRYPTION_KEY): probatio.All(
                        cv.string, probatio.Length(min=MIN_KEY_LENGTH)
                    ),
                    probatio.Optional(
                        CONF_LEASE_TTL, default=DEFAULT_LEASE_TTL
                    ): probatio.All(probatio.Coerce(float), probatio.Range(min=1.0)),
                    probatio.Optional(
                        CONF_RENEW_INTERVAL, default=DEFAULT_RENEW_INTERVAL
                    ): probatio.All(probatio.Coerce(float), probatio.Range(min=0.1)),
                    probatio.Optional(
                        CONF_UNLOAD_GRACE, default=DEFAULT_UNLOAD_GRACE
                    ): probatio.All(probatio.Coerce(float), probatio.Range(min=0.0)),
                    probatio.Optional(
                        CONF_RPC_TIMEOUT, default=DEFAULT_RPC_TIMEOUT
                    ): probatio.All(probatio.Coerce(float), probatio.Range(min=1.0)),
                    probatio.Optional(
                        CONF_REPLICAS, default=DEFAULT_REPLICAS
                    ): probatio.In([1, 3, 5]),
                    probatio.Optional(CONF_MANAGED_ADDONS, default=[]): probatio.All(
                        probatio.EnsureList(), [_MANAGED_ADDON]
                    ),
                    probatio.Optional(
                        CONF_ADDON_SNAPSHOT_INTERVAL,
                        default=DEFAULT_ADDON_SNAPSHOT_INTERVAL,
                    ): probatio.All(probatio.Coerce(float), probatio.Range(min=60.0)),
                    probatio.Optional(
                        CONF_ADDON_TAKEOVER_WAIT, default=DEFAULT_ADDON_TAKEOVER_WAIT
                    ): probatio.All(probatio.Coerce(float), probatio.Range(min=0.0)),
                    probatio.Optional(CONF_LOCAL_ADDONS, default=[]): probatio.All(
                        probatio.EnsureList(), [_ADDON_SLUG]
                    ),
                    probatio.Optional(CONF_ADDON_SEED_NODE): probatio.All(
                        cv.string, probatio.Match(NODE_ID_RE)
                    ),
                    # The name of a network storage of Supervisor (backup use).
                    probatio.Optional(CONF_ADDON_BACKUP_LOCATION): cv.string,
                    probatio.Optional(CONF_SKIP_ADDONS, default=[]): probatio.All(
                        probatio.EnsureList(), [cv.string]
                    ),
                    probatio.Optional(CONF_EXCLUDE_DOMAINS, default=[]): probatio.All(
                        probatio.EnsureList(), [cv.string]
                    ),
                    probatio.Optional(CONF_STORAGE_EXCLUDE, default=[]): probatio.All(
                        probatio.EnsureList(), [cv.string]
                    ),
                    probatio.Optional(
                        CONF_SECURITY, default=DEFAULT_SECURITY
                    ): probatio.In([SECURITY_STRICT, SECURITY_PERMISSIVE]),
                }
            ),
            _check_timing,
            _check_security,
            _check_addons,
        )
    },
    extra=probatio.ALLOW_EXTRA,
)


class ClusterNode:
    """This node: the config entry placement and everything it runs."""

    def __init__(self, hass: HomeAssistant, conf: ConfigType) -> None:
        """Initialize the node; nothing runs until started."""
        self.hass = hass
        self.node_id: str = conf[CONF_NODE_ID]
        self._exclude_domains = frozenset(conf[CONF_EXCLUDE_DOMAINS])
        self.coordinator = ClusterCoordinator(
            conf[CONF_NATS_URL],
            self.node_id,
            lease_ttl=conf[CONF_LEASE_TTL],
            replicas=conf[CONF_REPLICAS],
            user=conf.get(CONF_NATS_USER),
            password=conf.get(CONF_NATS_PASSWORD),
            token=conf.get(CONF_NATS_TOKEN),
            creds=conf.get(CONF_NATS_CREDS),
            ca_file=conf.get(CONF_NATS_CA),
            require_acl=conf[CONF_SECURITY] == SECURITY_STRICT,
            require_tls=conf[CONF_SECURITY] == SECURITY_STRICT
            and any(not _is_loopback(s) for s in conf[CONF_NATS_URL].split(",")),
        )
        self.leases = LeaseManager(
            hass,
            self.node_id,
            self.coordinator,
            ttl=conf[CONF_LEASE_TTL],
            renew_interval=conf[CONF_RENEW_INTERVAL],
            unload_grace=conf[CONF_UNLOAD_GRACE],
            exclude_domains=set(conf[CONF_EXCLUDE_DOMAINS]),
        )
        self.replicator = StateReplicator(
            hass, self.node_id, self.coordinator, self.leases
        )
        self.router = ServiceRouter(
            hass,
            self.node_id,
            self.coordinator,
            self.leases,
            self.replicator,
            rpc_timeout=conf[CONF_RPC_TIMEOUT],
        )
        self.storage = StorageSync(
            hass,
            self.node_id,
            self.coordinator,
            self.leases,
            exclude=set(conf[CONF_STORAGE_EXCLUDE]),
            cipher=(
                RecordCipher(conf[CONF_ENCRYPTION_KEY])
                if conf.get(CONF_ENCRYPTION_KEY)
                else None
            ),
        )
        self.leases.replicator = self.replicator
        self.leases.storage = self.storage
        self.events = EventReplicator(hass, self.node_id, self.coordinator)
        named_addons, manage_all = managed_addon_list(conf[CONF_MANAGED_ADDONS])
        self.addon_gate = SupervisorAddonGate(
            hass,
            self.node_id,
            self.leases,
            named_addons,
            manage_all=manage_all,
            local_addons=DEFAULT_LOCAL_ADDONS | set(conf[CONF_LOCAL_ADDONS]),
            seed_node=conf.get(CONF_ADDON_SEED_NODE),
            backup_location=conf.get(CONF_ADDON_BACKUP_LOCATION),
            backup_password=(
                backup_password(conf[CONF_ENCRYPTION_KEY])
                if conf.get(CONF_ENCRYPTION_KEY)
                else None
            ),
            skip_addons=conf[CONF_SKIP_ADDONS],
            nats_hosts=frozenset(
                host
                for server in conf[CONF_NATS_URL].split(",")
                if (host := urlsplit(server.strip()).hostname)
            ),
            store=(
                SnapshotStore(self.coordinator, BlobCipher(conf[CONF_ENCRYPTION_KEY]))
                if conf.get(CONF_ENCRYPTION_KEY)
                else None
            ),
            snapshot_interval=conf[CONF_ADDON_SNAPSHOT_INTERVAL],
            takeover_wait=conf[CONF_ADDON_TAKEOVER_WAIT],
        )
        self.leases.add_leader_listener(self.addon_gate.async_on_leader_change)
        self.coordinator.on_bucket_check = self._async_schedule_bucket_check
        self.coordinator.on_term_lost = self.leases.async_term_lost
        self.started = False
        self.stopping = False
        # Entries placed before NATS was reachable.
        self._waiting: set[str] = set()
        self._unregister_placement: CALLBACK_TYPE | None = None
        self._connect_task: asyncio.Task[None] | None = None
        self._bucket_task: asyncio.Task[None] | None = None

    async def async_place_entry(self, entry: ConfigEntry) -> str | None:
        """Core placement hook: decide whether ``entry`` sets up here."""
        if not self.leases.gates(entry):
            return None
        if not self.started:
            self._waiting.add(entry.entry_id)
            return STANDBY_WAITING_FOR_COORDINATOR
        return await self.leases.async_place_entry(entry)

    @property
    def serves_as_leader(self) -> bool:
        """Return True while this node serves as the leader.

        It leads, its promotion set up the clustered entries, and it is not
        shutting down. What the leader endpoint (``health.py``) answers.
        """
        return (
            self.started
            and not self.stopping
            and self.leases.serving
            and not self.leases.resigned
        )

    @callback
    def async_refuse_removal(self, entry: ConfigEntry) -> str | None:
        """Core placement hook: only the leader removes clustered entries.

        The removal runs the integration's clean-up (revoking credentials,
        deleting cloud resources), which the leader may still depend on. The
        leader publishes the removal; the standbys remove their copy then.
        """
        if entry.entry_id in self.storage.completing_removals:
            return None
        return self._refusal(entry, "remove")

    @callback
    def async_refuse_change(self, entry: ConfigEntry, change: str) -> str | None:
        """Core placement hook: options, reauth and the like are made on the leader.

        A change made on a standby is replaced by the leader's record at the
        next takeover, so it would be lost. The leader's own records, applied
        here, are let through.
        """
        if self.storage.applying_record:
            return None
        return self._refusal(entry, "change")

    def _refusal(self, entry: ConfigEntry, verb: str) -> str | None:
        if not self.leases.domain_is_clustered(entry.domain) or (
            self.leases.is_leader and not self.leases.resigned
        ):
            return None
        if self.leases.resigned:
            return "this cluster node is shutting down"
        if (leader := self.leases.leader()) is not None:
            return f"it runs on cluster node {leader}; {verb} it there"
        return f"this cluster node is not the leader; {verb} it on the leader"

    async def async_before_remove(self, entry: ConfigEntry) -> None:
        """Core placement hook: announce a removal before its clean-up runs."""
        if entry.entry_id in self.storage.completing_removals:
            return
        await self.storage.async_publish_removing(entry)

    def async_register(self) -> None:
        """Install the placement and give up leadership until elected.

        Replaces the fail-closed placement bootstrap installed for a
        configured cluster.
        """
        # Platforms of these domains are set up on every node, not on the leader.
        cluster_helper.async_set_every_instance_domains(
            self.hass, self._exclude_domains
        )
        self._unregister_placement = cluster_helper.async_take_over_placement(
            self.hass, self
        )

    async def async_start(self, connect_timeout: float) -> None:
        """Connect to NATS and start every part. Raises on failure.

        Only connecting is bounded by ``connect_timeout``. The rest is bounded
        by the JetStream timeout of each request and by the unload grace the
        first election may wait.
        """
        async with asyncio.timeout(connect_timeout):
            try:
                await self.coordinator.async_connect()
            except ConnectionRefused as err:
                # The server or this node's setup was refused: say why. It is
                # retried, and the repair goes away once a connection works.
                cluster_helper.async_report_connection_refused(self.hass, str(err))
                raise
        cluster_helper.async_clear_connection_refused(self.hass)
        async with asyncio.timeout(INITIAL_CONNECT_TIMEOUT):
            # The replicator and storage sync register their pre-setup hooks
            # before the lease manager can promote anything.
            await self.replicator.async_start()
            await self.storage.async_start()
            await self.events.async_start()
            await self.router.async_start()
            # Before the leases: the first member key reports the add-ons.
            await self.addon_gate.async_start()
            self.started = True
        await self.leases.async_start(self._waiting)
        self._waiting.clear()
        await self._async_reconcile()
        self.leases.async_enable_enforcement()

    @callback
    def _async_schedule_bucket_check(self) -> None:
        if self.stopping or (
            self._bucket_task is not None and not self._bucket_task.done()
        ):
            return
        self._bucket_task = self.hass.async_create_background_task(
            self._async_check_buckets(), f"cluster {self.node_id} bucket check"
        )

    async def _async_restart_term(self) -> None:
        """Start a term again after the term key was lost.

        Retried for a bounded time. A leader that cannot get a term writes
        nothing that the server accepts, so it stops leading: another node
        can take over and start one.
        """
        for _ in range(_TERM_RESTART_ATTEMPTS):
            if self.stopping or not self.leases.is_leader:
                return
            try:
                await self.coordinator.async_start_term(self.node_id)
            except Exception as err:  # noqa: BLE001 - retried, then given up
                _LOGGER.warning("Could not start a term again: %s", err)
                await asyncio.sleep(1)
            else:
                return
        if not self.stopping and self.leases.is_leader:
            _LOGGER.error(
                "Node %s could not start a term again; it stops leading",
                self.node_id,
            )
            self.leases.async_term_lost()

    async def _async_check_buckets(self) -> None:
        """Recreate buckets lost by a NATS restart and repopulate them."""
        for _ in range(30):  # JetStream may still be starting
            if self.stopping or not self.started or not self.coordinator.connected:
                return
            try:
                recreated = await self.coordinator.async_ensure_buckets()
            except Exception as err:  # noqa: BLE001 - retried
                _LOGGER.debug("Cluster bucket check failed: %s", err)
                await asyncio.sleep(1)
                continue
            if not recreated:
                if self.leases.is_leader and self.coordinator.fence_revision is None:
                    # The term key alone is gone (purged): this node still
                    # holds the lease, so it starts a term again, and publishes
                    # its states in it: receivers that replay the bucket drop
                    # the states of the older term.
                    await self._async_restart_term()
                    if self.leases.is_leader:
                        self.replicator.publish_owned()
                return
            _LOGGER.warning(
                "NATS had lost the cluster buckets %s; node %s recreated them",
                ", ".join(sorted(recreated)),
                self.node_id,
            )
            # Leases are re-elected through the normal CAS paths: a renewal of
            # a lost key fails, and the key is created again with the grace.
            self.leases.async_restart_watcher()
            if BUCKET_STORAGE in recreated and self.leases.is_leader:
                # The term key went with the bucket: start a term again, or
                # the server refuses every write of this leader.
                await self._async_restart_term()
            self.replicator.async_resync()
            self.storage.async_restart_watcher()
            await self.storage.async_republish()
            if BUCKET_STORAGE in recreated:
                await self.addon_gate.async_on_bucket_recreated()
            return

    async def _async_reconcile(self) -> None:
        """Handle clustered entries that set up before the placement existed.

        Bootstrap sets up the cluster first, so this only matters when the
        integration is set up late (tests, or a configuration change).
        """
        for entry in self.hass.config_entries.async_entries():
            if not self.leases.gates(entry) or entry.state not in _ACTIVE_STATES:
                continue
            if self.leases.is_leader:
                continue
            _LOGGER.warning(
                "%s (%s) was set up before the cluster but runs on another node;"
                " unloading it on node %s",
                entry.title,
                entry.domain,
                self.node_id,
            )
            await self.leases.async_demote(entry.entry_id)
        if self.leases.is_leader:
            self.replicator.publish_owned()
            # Records of entries held now (no pre-setup hook ran for these).
            try:
                await self.storage.async_flush()
            except HomeAssistantError as err:
                # The storage mirror keeps retrying what is still queued.
                _LOGGER.warning("Could not publish the config entries: %s", err)

    async def async_teardown(self, unload: bool = True) -> None:
        """Stop every part (also after a failed start).

        ``unload`` is False at shutdown: the lease is already released, and
        the entries were stopped by Core.
        """
        self.started = False
        if unload:
            gated = [
                e
                for e in self.hass.config_entries.async_entries()
                if self.leases.gates(e)
            ]
            await asyncio.gather(
                *(
                    self.leases.async_demote(e.entry_id)
                    for e in gated
                    if e.state in _ACTIVE_STATES
                )
            )
        await self.addon_gate.async_stop()
        await self.router.async_stop()
        await self.events.async_stop()
        await self.storage.async_stop()
        await self.replicator.async_stop()
        await self.leases.async_stop(release=False)
        await self.coordinator.async_close()

    def async_connect_in_background(self) -> None:
        """Keep trying to reach NATS; entries stay on standby meanwhile."""
        self._connect_task = self.hass.async_create_background_task(
            self._async_connect_loop(), f"cluster {self.node_id} connect"
        )

    async def _async_connect_loop(self) -> None:
        delay = 1.0
        while not self.stopping:
            await asyncio.sleep(delay)
            try:
                await self.async_start(INITIAL_CONNECT_TIMEOUT * 2)
            except Exception as err:  # noqa: BLE001 - any failure: retry
                # A configuration problem (not a connection one) is worth
                # seeing without debug logging.
                _LOGGER.log(
                    logging.WARNING
                    if isinstance(err, HomeAssistantError)
                    else logging.DEBUG,
                    "Cluster node %s still cannot start: %s",
                    self.node_id,
                    err,
                )
                await self.async_teardown()
                await self.async_hold_addons_offline()
                delay = min(delay * 2, 30.0)
                continue
            _LOGGER.info("Cluster node %s connected to NATS", self.node_id)
            return

    @callback
    def async_resign(self) -> None:
        """Stop acting as leader (Home Assistant is stopping)."""
        self.leases.async_resign()

    async def async_hand_over_addons(self) -> None:
        """Stop the managed add-ons and snapshot them (in the stop stage).

        While this node still holds the lease and its term (it is released
        at the close event): the snapshot is the one the next leader
        restores.
        """
        await self.addon_gate.async_handover()

    async def async_hold_addons_offline(self) -> None:
        """Keep the managed add-ons stopped while NATS cannot be reached."""
        if not self.stopping:
            await self.addon_gate.async_hold_offline()

    async def async_prepare_shutdown(self) -> None:
        """Stop the work of the leader before the integrations stop."""
        if self.started:
            # The add-ons keep running through the resign: they are stopped
            # and snapshotted in the stop stage (``async_hand_over_addons``),
            # which has the time for it; this stage has 20 s for everything.
            self.addon_gate.async_prepare_handover()
            await self.leases.async_prepare_shutdown()
        else:
            self.leases.async_resign()

    async def async_release(self) -> None:
        """Publish held entries, then hand the leases over.

        The leader lease is released only if every entry that runs on the
        leader was unloaded; otherwise the other nodes wait for it to expire.
        """
        self.stopping = True
        if self._connect_task is not None:
            self._connect_task.cancel()
        self.leases.async_resign()
        flush_ok = False
        if self.started:
            try:
                await self.storage.async_flush()
                flush_ok = True
            except Exception:
                _LOGGER.exception("Could not publish the config entries")
        await self.leases.async_stop(release=flush_ok)

    async def async_close(self) -> None:
        """Final teardown."""
        self.stopping = True
        if self._connect_task is not None:
            self._connect_task.cancel()
        await self.async_teardown(unload=False)
        cluster_helper.async_clear_connection_refused(self.hass)
        if self._unregister_placement is not None:
            self._unregister_placement()
            self._unregister_placement = None


async def async_setup_cluster(hass: HomeAssistant, conf: ConfigType) -> bool:
    """Set up this node as a cluster member from the ``cluster:`` key.

    Called by bootstrap before any integration. The fail-closed placement
    bootstrap installed stays if the configuration is invalid.
    """
    try:
        conf = CONFIG_SCHEMA({DOMAIN: conf})[DOMAIN]
    except probatio.Invalid as err:
        _LOGGER.error("Invalid cluster configuration: %s", err)
        # The path only: the message of a check can quote a value (a URL with
        # a password). The log has the whole text.
        path = [str(part) for part in err.path if str(part) != DOMAIN]
        if path:
            reason = f"the configuration is invalid at `{' > '.join(path)}`"
        else:
            # A check of the whole section (strict mode, timing): its own text,
            # which names what to configure and never quotes a value.
            reason = f"the configuration is invalid: {err.error_message}"
        cluster_helper.async_report_setup_failed(hass, reason)
        return False
    node = ClusterNode(hass, conf)
    hass.data[DATA_NODE] = node
    # Before any await: no clustered entry may start without the placement.
    node.async_register()

    async def _async_http_ready(hass: HomeAssistant, _: str) -> None:
        await async_register_leader_view(hass, node)

    async_when_setup(hass, "http", _async_http_ready)

    try:
        await node.async_start(INITIAL_CONNECT_TIMEOUT)
    except Exception as err:  # noqa: BLE001 - keep trying in the background
        _LOGGER.error(
            "Cluster node %s cannot start (%s); its config entries stay on"
            " standby until it can reach NATS",
            node.node_id,
            err or type(err).__name__,
        )
        await node.async_teardown()
        await node.async_hold_addons_offline()
        node.async_connect_in_background()

    # Before the stop event: entries are unloaded while their integrations
    # still expect it, so that a clean release is possible at the end.
    hass.async_add_shutdown_job(HassJob(node.async_prepare_shutdown))

    @callback
    def _async_stop(_: Event) -> None:
        node.async_resign()
        # A tracked task: the stop stage waits for it.
        hass.async_create_task(
            node.async_hand_over_addons(), "cluster add-on handover", eager_start=True
        )

    async def _async_close(_: Event) -> None:
        # After the integrations stopped and the final save ran.
        await node.async_release()
        await node.async_close()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_stop)
    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_CLOSE, _async_close)
    return True
