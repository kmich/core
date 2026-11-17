"""Run Supervisor add-ons (apps) on the leader only, and move their data with them.

An add-on such as Zigbee2MQTT, Mosquitto or Music Assistant runs on the
leader's host, next to the config entries that use it. Running one on two
hosts at once can be harmful (two Zigbee2MQTT on one network coordinator
corrupt the network); starting one with another host's or no data can be
worse (a Zigbee2MQTT without the network's coordinator backup forms a new
network on the coordinator). The gate therefore:

* manages the add-ons of ``managed_addons``: named ones, or ``all`` the
  installed add-ons except the ``local_addons`` (tools that run on every
  host: SSH, Samba, an editor), in the order of their startup stage;
* starts a managed add-on only on the leader, and only once no other node
  may still run it: each node reports in its member key the managed add-ons
  it may be running; a node that ran one and stopped reporting (a dead host,
  or a dead Home Assistant on a live host) is waited for ``takeover_wait``;
* starts a replicated add-on only with the data of the cluster's latest
  snapshot (restored before the start). Without a snapshot only its seed node
  starts it (whose copy is declared authoritative), or the leader, for an
  add-on that was installed after the cluster knew its add-ons;
* snapshots replicated add-ons on the leader every ``snapshot_interval`` and,
  stopped, at a planned handover (see ``addon_snapshot.py``). The add-on's
  Supervisor backup goes through NATS, or to a network storage of Supervisor
  (``backup_location``) for large add-ons; the folders of the configuration
  folder an add-on keeps data in (named in its options, or ``paths``) always
  go through NATS;
* stops managed add-ons on every other node, sets them to manual boot so
  that Supervisor does not start them with the host, and checks every
  ``_STANDBY_CHECK_INTERVAL`` that they stay stopped;
* when the watchdog ends a hung process, has a detached helper process stop
  them (the watchdog does not wait for it).

A crash loses what changed since the last snapshot. Everything runs in the
background: the gate never delays a change of leadership. Supervisor (HA OS
or a supervised installation) is required on every node; elsewhere the gate
does nothing.
"""

import asyncio
import contextlib
from dataclasses import dataclass
import logging
import os
from pathlib import Path
import subprocess
import sys
from time import monotonic
from typing import Any, Final

import aiohttp

from homeassistant.core import STOP_STAGE_SHUTDOWN_TIMEOUT, HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir, storage
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .addon_snapshot import (
    SnapshotError,
    SnapshotStore,
    commit_swap,
    pack,
    paths_from_options,
    remove_staging,
    rollback_swap,
    swap_in,
    unpack,
)
from .const import (
    ADDON_ALL,
    ADDON_SLUG_RE,
    CONF_EXCLUDE,
    CONF_PATHS,
    CONF_REPLICATE,
    CONF_SEED_NODE,
    CONF_SLUG,
    DOMAIN,
    LEADER_KEY,
)
from .coordinator import NoTerm, TermSuperseded
from .lease_manager import LeaseManager

_LOGGER = logging.getLogger(__name__)

SUPERVISOR_URL: Final = "http://supervisor"
STORE_KEY: Final = "cluster.addons"
STORE_VERSION: Final = 1
ISSUE_DOMAIN: Final = "homeassistant"
ISSUE_NOT_STARTED: Final = "cluster_addon_{slug}"

# Supervisor's startup stages, in the order it starts them.
_STAGES: Final = ("initialize", "system", "services", "application", "once")
_RUNNING_STATES: Final = frozenset({"started", "startup"})
_STANDBY_CHECK_INTERVAL: Final = 30.0
_STANDBY_BOOT_POLL: Final = 0.25
_RETRY_INTERVAL: Final = 30.0
_RESOLVE_INTERVAL: Final = 60.0
_WAIT_POLL: Final = 2.0
# The handover runs in Core's stop stage, which waits STOP_STAGE_SHUTDOWN_TIMEOUT
# (100 s) for its tasks: it must end before that, with time to spare.
_HANDOVER_TIMEOUT: Final = STOP_STAGE_SHUTDOWN_TIMEOUT - 20.0
# A live node that still reports an add-on this long raises the repair issue.
_BLOCKED_ISSUE_AFTER: Final = 120.0
# The helper process that stops add-ons after the watchdog ended this one:
# ``python -I -c _STOP_HELPER <supervisor url> <slug>...``, the token in its
# environment (not on its command line).
_STOP_HELPER: Final = """
import os, sys, urllib.request
for slug in sys.argv[2:]:
    request = urllib.request.Request(
        sys.argv[1] + "/addons/" + slug + "/stop",
        data=b"",
        method="POST",
        headers={"Authorization": "Bearer " + os.environ["SUPERVISOR_TOKEN"]},
    )
    try:
        urllib.request.urlopen(request, timeout=120).close()
    except Exception as err:
        sys.stderr.write("Could not stop add-on %s: %s\\n" % (slug, err))
"""
_SHORT_TIMEOUT: Final = aiohttp.ClientTimeout(total=60)
_LONG_TIMEOUT: Final = aiohttp.ClientTimeout(total=3600)


class SupervisorError(HomeAssistantError):
    """A Supervisor request failed."""


class NotReady(HomeAssistantError):
    """An add-on cannot be started here yet; the message says why."""


@dataclass(frozen=True)
class ManagedAddon:
    """One managed add-on."""

    slug: str
    # Folders of the configuration folder the add-on keeps data in; None:
    # the ones its options name.
    paths: tuple[str, ...] | None = None
    exclude: tuple[str, ...] = ()
    # False: only started and stopped, never snapshotted (and so started
    # with whatever data the node has).
    replicate: bool = True
    seed_node: str | None = None


@dataclass
class _State:
    """What the gate knows of one add-on on this node."""

    # Set when this node started it as the leader with current data.
    led: bool = False
    issue: str | None = None
    waiting_since: float | None = None


class SupervisorAddonGate:
    """Starts managed add-ons on the leader, stops them elsewhere, moves their data."""

    def __init__(
        self,
        hass: HomeAssistant,
        node_id: str,
        leases: LeaseManager,
        addons: list[ManagedAddon],
        *,
        manage_all: bool = False,
        local_addons: frozenset[str] = frozenset(),
        seed_node: str | None = None,
        skip_addons: list[str] | None = None,
        store: SnapshotStore | None = None,
        backup_location: str | None = None,
        backup_password: str | None = None,
        nats_hosts: frozenset[str] = frozenset(),
        snapshot_interval: float,
        takeover_wait: float,
    ) -> None:
        """Initialize the gate; nothing runs until ``async_start``."""
        self.hass = hass
        self.node_id = node_id
        self.leases = leases
        self.manage_all = manage_all
        self.local_addons = local_addons
        self.seed_node = seed_node
        self._explicit = {a.slug: a for a in addons if a.slug not in local_addons}
        # The managed add-ons, in start order (``async_resolve``).
        self.addons: list[ManagedAddon] = list(self._explicit.values())
        self.skip_addons = frozenset(skip_addons or ())
        self.store = store
        self.backup_location = backup_location
        self.backup_password = backup_password
        # Host names of the NATS servers: an add-on with one of them is the
        # cluster's own infrastructure and runs on every node.
        self.nats_hosts = nats_hosts
        self.snapshot_interval = snapshot_interval
        self.takeover_wait = takeover_wait
        self.supervisor_token = os.environ.get("SUPERVISOR_TOKEN")
        self._states: dict[str, _State] = {}
        # Managed add-ons that may run on this node: reported to the others.
        # Unknown counts as running until Supervisor said otherwise.
        self.maybe_running: set[str] = set(self._explicit)
        # Home Assistant stops on this leader: the add-ons keep running until
        # ``async_handover`` stops and snapshots them.
        self._handover_pending = False
        # A copy of what to stop, in stop order, for the watchdog thread: it
        # must not read the sets the event loop changes. Replaced whole.
        self._exit_slugs: tuple[str, ...] = ()
        # The snapshot each add-on's local data is (made or restored here).
        self.local_snapshot: dict[str, str] = {}
        # The cluster's add-on index as last read (None: none or unknown).
        self._index: dict[str, Any] | None = None
        self._index_known = False
        self._stages: dict[str, str] = {}
        self._hostnames: dict[str, str] = {}
        # Add-ons that are local because they serve the cluster (NATS).
        self._infra_local: frozenset[str] = frozenset()
        # The add-ons installed here, once Supervisor listed them.
        self._installed: frozenset[str] | None = None
        self._marker = storage.Store[dict[str, Any]](
            hass, STORE_VERSION, STORE_KEY, private=True
        )
        self._started_at = monotonic()
        self._leading = False
        self._lead_task: asyncio.Task[None] | None = None
        self._snapshot_task: asyncio.Task[None] | None = None
        self._standby_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self.started = False

    @property
    def is_haos(self) -> bool:
        """Return True when running under the Supervisor."""
        return bool(self.supervisor_token)

    @property
    def active(self) -> bool:
        """Return True if there is anything to manage here."""
        return (bool(self._explicit) or self.manage_all) and self.is_haos

    def _state_of(self, slug: str) -> _State:
        return self._states.setdefault(slug, _State())

    # Supervisor API

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        data: aiohttp.FormData | None = None,
        timeout: aiohttp.ClientTimeout = _SHORT_TIMEOUT,
        raw: bool = False,
    ) -> Any:
        session = async_get_clientsession(self.hass)
        headers = {"Authorization": f"Bearer {self.supervisor_token}"}
        try:
            async with session.request(
                method,
                f"{SUPERVISOR_URL}{path}",
                headers=headers,
                json=json_body,
                data=data,
                timeout=timeout,
            ) as resp:
                if raw and resp.status == 200:
                    return await resp.read()
                try:
                    body = await resp.json(content_type=None)
                except ValueError:
                    body = {}
                if resp.status != 200 or (
                    isinstance(body, dict) and body.get("result") == "error"
                ):
                    message = body.get("message") if isinstance(body, dict) else None
                    raise SupervisorError(
                        f"Supervisor {method} {path} returned HTTP {resp.status}"
                        + (f": {message}" if message else "")
                    )
                return body.get("data", {}) if isinstance(body, dict) else {}
        except (aiohttp.ClientError, TimeoutError) as err:
            raise SupervisorError(f"Supervisor {method} {path} failed: {err}") from err

    async def _info(self, slug: str) -> dict[str, Any]:
        info = await self._request("GET", f"/addons/{slug}/info")
        if not isinstance(info, dict):
            return {}
        if isinstance(stage := info.get("startup"), str):
            self._stages[slug] = stage
        if isinstance(hostname := info.get("hostname"), str):
            self._hostnames[slug] = hostname
        return info

    async def _state(self, slug: str) -> str | None:
        return (await self._info(slug)).get("state")

    async def _call(self, slug: str, action: str) -> bool:
        """Start or stop ``slug``; return False (logged) if Supervisor refused."""
        try:
            await self._request(
                "POST", f"/addons/{slug}/{action}", timeout=_LONG_TIMEOUT
            )
        except SupervisorError as err:
            _LOGGER.warning("Could not %s add-on %s: %s", action, slug, err)
            return False
        return True

    async def _async_manual_boot(self, slug: str, info: dict[str, Any]) -> None:
        """Keep Supervisor from starting the add-on with the host."""
        if info.get("boot") == "manual":
            return
        try:
            await self._request(
                "POST", f"/addons/{slug}/options", json_body={"boot": "manual"}
            )
        except SupervisorError as err:
            _LOGGER.warning("Could not set add-on %s to manual boot: %s", slug, err)
        else:
            _LOGGER.info("Set add-on %s to manual boot: the cluster starts it", slug)

    async def _async_stop_confirmed(self, slug: str) -> bool:
        """Stop ``slug`` and return True once Supervisor reports it stopped.

        An add-on that is not installed here is stopped.
        """
        try:
            info = await self._info(slug)
            if info.get("state") in _RUNNING_STATES:
                await self._request(
                    "POST", f"/addons/{slug}/stop", timeout=_LONG_TIMEOUT
                )
                info = await self._info(slug)
        except SupervisorError as err:
            if self._installed is not None and slug not in self._installed:
                # Not installed here (Supervisor listed the installed ones).
                self.maybe_running.discard(slug)
                return True
            _LOGGER.warning("Could not stop add-on %s: %s", slug, err)
            return False
        await self._async_manual_boot(slug, info)
        if info.get("state") in _RUNNING_STATES:
            return False
        self.maybe_running.discard(slug)
        return True

    # Which add-ons

    async def async_resolve(self) -> None:
        """Work out the managed add-ons and their order.

        Named add-ons always; with ``all`` also every add-on installed here
        and every one in the cluster's index (installed on the leader, so the
        restore installs it here). Ordered by startup stage, as Supervisor
        starts them, then as named.
        """
        try:
            listing = await self._request("GET", "/addons")
        except SupervisorError as err:
            _LOGGER.debug("Could not list the add-ons: %s", err)
            listing = None
        if isinstance(listing, dict) and isinstance(
            items := listing.get("addons"), list
        ):
            self._installed = frozenset(
                item["slug"]
                for item in items
                if isinstance(item, dict)
                and isinstance(item.get("slug"), str)
                and ADDON_SLUG_RE.match(item["slug"])
            )
        installed = self._installed or frozenset()
        await self._async_read_index()
        for slug in installed - self.local_addons:
            if slug not in self._stages or slug not in self._hostnames:
                with contextlib.suppress(SupervisorError):
                    await self._info(slug)
        infra = frozenset(
            slug for slug in installed if self._hostnames.get(slug) in self.nats_hosts
        )
        if infra - self._infra_local:
            _LOGGER.info(
                "Add-ons %s serve the cluster's NATS: they run on every node",
                ", ".join(sorted(infra - self._infra_local)),
            )
        self._infra_local = infra
        slugs = set(self._explicit)
        if self.manage_all:
            slugs |= installed | set(self._index or ()) | set(self._peer_installed())
        slugs -= self.local_addons | infra
        for slug in slugs:
            if slug not in self._stages and isinstance(
                stage := self._index_entry(slug).get("stage"), str
            ):
                self._stages[slug] = stage
        named = list(self._explicit)

        def _order(slug: str) -> tuple[int, int, str]:
            stage = self._stages.get(slug, "application")
            rank = _STAGES.index(stage) if stage in _STAGES else 3
            return (rank, named.index(slug) if slug in named else len(named), slug)

        resolved = [
            self._explicit.get(slug) or ManagedAddon(slug)
            for slug in sorted(slugs, key=_order)
        ]
        for addon in resolved:
            if addon.slug not in {a.slug for a in self.addons} and (
                addon.slug in installed or addon.slug in self._explicit
            ):
                # New here: running until Supervisor says otherwise.
                self.maybe_running.add(addon.slug)
        self.addons = resolved
        self._note_exit_slugs()

    async def _async_read_index(self) -> None:
        if self.store is None:
            return
        try:
            self._index = await self.store.async_read_index()
        except SnapshotError as err:
            _LOGGER.debug("Could not read the add-on index: %s", err)
            self._index_known = False
            return
        self._index_known = True

    def _index_entry(self, slug: str) -> dict[str, Any]:
        """Return the index entry of ``slug``: its stage and who holds its data."""
        entry = (self._index or {}).get(slug)
        if isinstance(entry, str):
            return {"stage": entry}
        return entry if isinstance(entry, dict) else {}

    def _peer_installed(self) -> dict[str, list[str]]:
        """Return, per add-on, the other nodes that have it installed.

        From each node's last member key, also of a node that went away: it
        may come back with its copy.
        """
        found: dict[str, list[str]] = {}
        for node, (holder, _, _) in sorted(self.leases.peer_member_reports().items()):
            installed = holder.get("installed")
            for slug in installed if isinstance(installed, list) else []:
                if isinstance(slug, str) and ADDON_SLUG_RE.match(slug):
                    found.setdefault(slug, []).append(node)
        return found

    async def _async_note_in_index(self, slug: str) -> None:
        """Add ``slug`` to the cluster's index (the leader, after its snapshot)."""
        if self.store is None or not self._index_known:
            return
        index = dict(self._index or {})
        entry = {"stage": self._stages.get(slug, "application"), "held_by": None}
        if index.get(slug) == entry:
            return
        index[slug] = entry
        await self.store.async_write_index(index)
        self._index = index

    async def _async_note_held(self) -> None:
        """Record add-ons that only other nodes have, without a snapshot.

        Their data is on that node. A copy installed here later must not
        become the cluster's: the index names the node that holds the data.
        """
        if self.store is None or not self._index_known or self._installed is None:
            return
        index = dict(self._index or {})
        changed = False
        for slug, nodes in self._peer_installed().items():
            if slug in self._installed or slug in self.local_addons:
                continue
            if not (self.manage_all or slug in self._explicit):
                continue
            entry = self._index_entry(slug)
            if entry.get("held_by") is not None:
                continue
            if await self.store.async_read_manifest(slug) is not None:
                continue
            index[slug] = {"stage": entry.get("stage"), "held_by": nodes[0]}
            changed = True
        if changed:
            await self.store.async_write_index(index)
            self._index = index

    # Life cycle

    async def async_start(self, at_once: bool = False) -> None:
        """Read what this node's data is, report, and keep standbys stopped.

        Called before the lease manager starts, so the first report is in
        the first member key. ``at_once``: stop the managed add-ons without
        waiting for an election (``async_hold_offline``).
        """
        if not self.active:
            return
        data = await self._marker.async_load() or {}
        self.local_snapshot = {
            slug: snapshot_id
            for slug, snapshot_id in data.get("snapshots", {}).items()
            if isinstance(snapshot_id, str)
        }
        await self.async_resolve()
        self._started_at = monotonic()
        self.leases.member_extra = self.member_extra
        self.leases.add_before_exit(self.stop_on_exit)
        self.started = True
        if not self._leading:
            self._start_standby_loop(at_once)

    async def async_hold_offline(self) -> None:
        """Keep the managed add-ons stopped on a node that cannot reach NATS.

        Such a node cannot lead, and it cannot see whether another node runs
        them: an add-on that still runs here (Home Assistant restarted, the
        add-ons did not) is stopped at once, as the entries stay unloaded.
        Only Supervisor is needed; the add-on index is read once NATS is
        reachable again.
        """
        if not self.active or self.started:
            return
        try:
            await self.async_start(at_once=True)
        except Exception as err:  # noqa: BLE001 - tried again with the next attempt
            _LOGGER.warning("Could not keep the add-ons stopped: %s", err)

    async def async_stop(self) -> None:
        """Stop the background work; a node that still leads stops its add-ons."""
        was_leading = self.started and self._leading
        self._leading = False
        self.started = False
        for task in (self._lead_task, self._snapshot_task, self._standby_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        self._lead_task = self._snapshot_task = self._standby_task = None
        if was_leading:
            with contextlib.suppress(Exception):
                for addon in reversed(self.addons):
                    await self._async_stop_confirmed(addon.slug)

    @callback
    def member_extra(self) -> dict[str, Any]:
        """Fields of this node's member key: the add-ons that may run here."""
        if not self.active:
            return {}
        self._note_exit_slugs()
        extra: dict[str, Any] = {"addons": sorted(self.maybe_running)}
        if self._installed is not None:
            # What this node has a copy of (seeding must not overrule it).
            extra["installed"] = sorted(
                self._installed - self.local_addons - self._infra_local
            )
        return extra

    async def async_on_leader_change(self, leader: bool) -> None:
        """Leader listener registered with the lease manager. Never blocks."""
        if not self.active or not self.started:
            return
        if not leader and self._handover_pending:
            # Home Assistant stops: the handover stops and snapshots them.
            self._leading = False
            for task in (self._lead_task, self._snapshot_task):
                if task is not None and not task.done():
                    task.cancel()
            self._lead_task = self._snapshot_task = None
            return
        self._leading = leader
        if leader:
            if self._standby_task is not None:
                self._standby_task.cancel()
                self._standby_task = None
            if self._lead_task is None or self._lead_task.done():
                self._lead_task = self.hass.async_create_background_task(
                    self._async_lead(), f"cluster {self.node_id} add-ons"
                )
            return
        for task in (self._lead_task, self._snapshot_task):
            if task is not None and not task.done():
                task.cancel()
        self._lead_task = self._snapshot_task = None
        for state in self._states.values():
            state.led = False
        self._start_standby_loop(at_once=True)

    def _start_standby_loop(self, at_once: bool = False) -> None:
        if self._standby_task is None or self._standby_task.done():
            self._standby_task = self.hass.async_create_background_task(
                self._async_standby_loop(at_once),
                f"cluster {self.node_id} add-on standby",
            )

    async def _async_standby_loop(self, at_once: bool) -> None:
        if not at_once:
            # Give a node that is about to win the election the chance to,
            # unless another node leads already: then stop them at once.
            until = monotonic() + 2 * self.leases.ttl
            while (
                not self._leading
                and monotonic() < until
                and self.leases.remote_owner(LEADER_KEY) is None
            ):
                await asyncio.sleep(_STANDBY_BOOT_POLL)
        while not self._leading:
            await self.async_resolve()
            await self.async_enforce_standby()
            await asyncio.sleep(_STANDBY_CHECK_INTERVAL)

    async def async_enforce_standby(self) -> None:
        """Stop every managed add-on that runs here (this node does not lead)."""
        async with self._lock:
            before = set(self.maybe_running)
            for addon in reversed(self.addons):
                if self._leading:
                    return
                if await self._async_stop_confirmed(addon.slug):
                    self._clear_issue(addon.slug)
            if self.maybe_running != before:
                await self.leases.async_publish_member()

    # Leading

    def blocked_by(self, slug: str) -> str | None:
        """Return why ``slug`` may still run on another node, or None."""
        now = monotonic()
        for node, (holder, seen, live) in self.leases.peer_member_reports().items():
            running = holder.get("addons")
            if not isinstance(running, list) or slug not in running:
                continue
            if live:
                return f"it may still run on node {node}"
            if (left := self.takeover_wait - (now - seen)) > 0:
                return (
                    f"node {node} ran it and has not reported for {now - seen:.0f}s;"
                    f" waiting {left:.0f}s more"
                )
        if (left := self.takeover_wait - (now - self._started_at)) > 0:
            # A node that went away before this one started never reported here.
            return f"this node started recently; waiting {left:.0f}s for reports"
        return None

    def _still_leading(self) -> bool:
        return self._leading and self.leases.is_leader

    async def _async_lead(self) -> None:
        """Start the managed add-ons in order, again for new ones; snapshot them."""
        if self.store is not None:
            self._snapshot_task = self.hass.async_create_background_task(
                self._async_snapshot_loop(), f"cluster {self.node_id} add-on snapshots"
            )
        while self._still_leading():
            await self.async_resolve()
            with contextlib.suppress(SnapshotError, NoTerm, TermSuperseded):
                await self._async_note_held()
            waiting = failed = False
            for addon in self.addons:
                if addon.slug in self.skip_addons or self._state_of(addon.slug).led:
                    continue
                if not self._still_leading():
                    return
                result = await self._async_try_lead(addon)
                waiting |= result == "waiting"
                failed |= result == "failed"
            await asyncio.sleep(
                _WAIT_POLL
                if waiting
                else _RETRY_INTERVAL
                if failed
                else _RESOLVE_INTERVAL
            )

    async def _async_try_lead(self, addon: ManagedAddon) -> str:
        """Try once to start ``addon`` here; return started, waiting or failed."""
        slug = addon.slug
        state = self._state_of(slug)
        if (reason := self.blocked_by(slug)) is not None:
            _LOGGER.debug("Not starting add-on %s yet: %s", slug, reason)
            if state.waiting_since is None:
                state.waiting_since = monotonic()
            if (
                reason.startswith("it may still run on node")
                and monotonic() - state.waiting_since > _BLOCKED_ISSUE_AFTER
            ):
                # A live node that does not report it stopped (its Supervisor
                # does not answer, or will not stop it).
                self._report_issue(slug, reason)
            return "waiting"
        state.waiting_since = None
        # Reported before anything may start it (a restore can).
        if slug not in self.maybe_running:
            self.maybe_running.add(slug)
            await self.leases.async_publish_member()
        try:
            async with self._lock:
                if not await self._async_start_here(addon):
                    return "waiting"
        except (
            NotReady,
            SnapshotError,
            SupervisorError,
            NoTerm,
            TermSuperseded,
        ) as err:
            self._report_issue(slug, str(err))
            await self._async_refresh_running(slug)
            return "failed"
        state.led = True
        self._clear_issue(slug)
        _LOGGER.info("Started add-on %s on cluster node %s", slug, self.node_id)
        return "started"

    async def _async_refresh_running(self, slug: str) -> None:
        """Report what runs after a failed attempt (it may not be installed)."""
        try:
            running = (await self._state(slug)) in _RUNNING_STATES
        except SupervisorError:
            running = self._installed is None or slug in self._installed
        if not running and slug in self.maybe_running:
            self.maybe_running.discard(slug)
            await self.leases.async_publish_member()

    async def _async_start_here(self, addon: ManagedAddon) -> bool:
        """Bring the data up to date and start ``addon``; False if no longer leading."""
        slug = addon.slug
        await self._async_prepare(addon)
        if not (self._leading and self.leases.is_leader):
            return False
        await self._async_manual_boot(slug, await self._info(slug))
        if not await self._call(slug, "start"):
            raise SupervisorError("Supervisor did not start it")
        if (await self._state(slug)) not in _RUNNING_STATES:
            raise SupervisorError("it did not stay started")
        return True

    def seed_blocker(self, addon: ManagedAddon) -> str | None:
        """Return why this node's data of ``addon`` may not become the cluster's.

        None if it may: this node is the add-on's own seed node; the index
        says this node holds its data; this node is the seed node of all
        add-ons and no other node holds this one's data; or nobody else has
        it installed and it is new to the cluster (not in its index).
        """
        slug, me = addon.slug, self.node_id
        if addon.seed_node is not None:
            if addon.seed_node == me:
                return None
            return f"the cluster has no snapshot of it yet; its seed node is {addon.seed_node}"
        held = self._index_entry(slug).get("held_by")
        if isinstance(held, str) and held != me:
            return (
                f"node {held} has its data and the cluster has no snapshot of it:"
                f" let node {held} lead once, or set seed_node: {me} for it to"
                f" make this node's copy the cluster's (node {held}'s is then"
                " replaced)"
            )
        if held == me:
            return None
        if self.seed_node is not None:
            if self.seed_node == me:
                return None
            return (
                "the cluster has no snapshot of it yet; the seed node is"
                f" {self.seed_node}: let it lead once"
            )
        others = self._peer_installed().get(slug, [])
        if others:
            return (
                f"the cluster has no snapshot of it yet, and node {others[0]} has"
                f" it installed too: let the node whose copy is right lead once"
                " with addon_seed_node (or seed_node of the add-on) set to it"
            )
        if (
            self._index_known
            and self._index is not None
            and slug not in self._index
            and self._installed is not None
            and slug in self._installed
        ):
            # Installed after the cluster knew its add-ons, here only.
            return None
        return (
            "the cluster has no snapshot of it yet. Set addon_seed_node (or"
            " seed_node of the add-on) to the node whose copy of its data is the"
            " right one, and let that node lead once"
        )

    async def _async_prepare(self, addon: ManagedAddon) -> None:
        """Make sure this node has the cluster's latest data of ``addon``."""
        if not addon.replicate:
            return
        if self.store is None:
            raise NotReady("snapshots need the cluster's encryption_key")
        slug = addon.slug
        manifest = await self.store.async_read_manifest(slug)
        if manifest is None:
            if (reason := self.seed_blocker(addon)) is not None:
                raise NotReady(reason)
            if self._installed is not None and slug not in self._installed:
                raise NotReady(
                    "the cluster has no snapshot of it, and it is not installed here"
                )
            _LOGGER.warning(
                "No snapshot of add-on %s in the cluster: node %s's data becomes"
                " the cluster's copy",
                slug,
                self.node_id,
            )
            await self._async_snapshot(addon)
            return
        if self.local_snapshot.get(slug) == manifest.get("id"):
            return
        await self._async_restore(addon, manifest)

    async def _async_restore(
        self, addon: ManagedAddon, manifest: dict[str, Any]
    ) -> None:
        """Replace this node's data of ``addon`` by the snapshot."""
        if self.store is None:
            raise NotReady("snapshots need the cluster's encryption_key")
        slug = addon.slug
        _LOGGER.info(
            "Restoring add-on %s from the snapshot of node %s",
            slug,
            manifest.get("node"),
        )
        data = await self.store.async_read(slug, manifest)
        config_dir = Path(self.hass.config.config_dir)
        unpacked = await self.hass.async_add_executor_job(
            unpack, data, config_dir, slug
        )
        try:
            if not await self._async_stop_confirmed(slug):
                raise SupervisorError("it could not be stopped for the restore")
            # Reported again: the restore can start it.
            self.maybe_running.add(slug)
            await self.leases.async_publish_member()
            swap = await self.hass.async_add_executor_job(swap_in, unpacked, config_dir)
            try:
                if isinstance(backup := manifest.get("backup"), str):
                    await self._async_restore_from_location(
                        slug, backup, manifest.get("location")
                    )
                elif unpacked.supervisor_backup is not None:
                    content = await self.hass.async_add_executor_job(
                        unpacked.supervisor_backup.read_bytes
                    )
                    await self._async_supervisor_restore(slug, content)
            except BaseException:
                # The add-on's own data was not restored: its folders go
                # back too, so the two match (the restore is tried again).
                await self.hass.async_add_executor_job(rollback_swap, swap)
                raise
            await self.hass.async_add_executor_job(commit_swap, swap)
        finally:
            await self.hass.async_add_executor_job(remove_staging, unpacked)
        await self._async_remember(slug, str(manifest["id"]))

    async def _async_restore_from_location(
        self, slug: str, backup: str, location: Any
    ) -> None:
        """Restore ``slug`` from a backup on the cluster's network storage."""
        await self._request("POST", "/backups/reload", timeout=_LONG_TIMEOUT)
        body: dict[str, Any] = {"addons": [slug], "background": False}
        if isinstance(location, str):
            body["location"] = location
        if self.backup_password is not None:
            body["password"] = self.backup_password
        try:
            await self._request(
                "POST",
                f"/backups/{backup}/restore/partial",
                json_body=body,
                timeout=_LONG_TIMEOUT,
            )
        except SupervisorError as err:
            raise SupervisorError(
                f"the backup {backup} on {location} could not be restored (is"
                f" that network storage mounted on this host?): {err}"
            ) from err

    async def _async_supervisor_restore(self, slug: str, content: bytes) -> None:
        form = aiohttp.FormData()
        form.add_field(
            "file", content, filename="snapshot.tar", content_type="application/x-tar"
        )
        uploaded = await self._request(
            "POST", "/backups/new/upload", data=form, timeout=_LONG_TIMEOUT
        )
        backup = uploaded.get("slug") if isinstance(uploaded, dict) else None
        if not isinstance(backup, str):
            raise SupervisorError("Supervisor did not name the uploaded backup")
        try:
            await self._request(
                "POST",
                f"/backups/{backup}/restore/partial",
                json_body={"addons": [slug], "background": False},
                timeout=_LONG_TIMEOUT,
            )
        finally:
            with contextlib.suppress(SupervisorError):
                await self._request("DELETE", f"/backups/{backup}")

    async def _async_new_backup(self, slug: str, location: str | None) -> str:
        body: dict[str, Any] = {
            "name": f"Cluster snapshot of {slug}",
            "addons": [slug],
            "homeassistant": False,
            "compressed": True,
            "background": False,
        }
        if location is not None:
            body["location"] = location
            if self.backup_password is not None:
                body["password"] = self.backup_password
        created = await self._request(
            "POST", "/backups/new/partial", json_body=body, timeout=_LONG_TIMEOUT
        )
        backup = created.get("slug") if isinstance(created, dict) else None
        if not isinstance(backup, str):
            raise SupervisorError("Supervisor did not name the backup it made")
        return backup

    async def _async_delete_backup(self, backup: str, location: str | None) -> None:
        with contextlib.suppress(SupervisorError):
            await self._request(
                "DELETE",
                f"/backups/{backup}",
                json_body={"location": location} if location is not None else None,
            )

    async def _async_supervisor_backup(self, slug: str) -> bytes:
        backup = await self._async_new_backup(slug, None)
        try:
            content = await self._request(
                "GET", f"/backups/{backup}/download", timeout=_LONG_TIMEOUT, raw=True
            )
        finally:
            await self._async_delete_backup(backup, None)
        if not isinstance(content, bytes):
            raise SupervisorError("Supervisor did not send the backup")
        return content

    async def _async_snapshot(self, addon: ManagedAddon) -> None:
        """Snapshot this node's data of ``addon`` as the cluster's copy."""
        if self.store is None or not addon.replicate:
            return
        slug = addon.slug
        info = await self._info(slug)
        paths = (
            list(addon.paths)
            if addon.paths is not None
            else paths_from_options(info.get("options"))
        )
        extra: dict[str, Any] = {"addon": slug, "addon_version": info.get("version")}
        location = self.backup_location
        backup: str | None = None
        content: bytes | None = None
        previous = await self.store.async_read_manifest(slug)
        if location is not None:
            # Large add-ons: the Supervisor backup stays on the network
            # storage; NATS holds the manifest and the folders.
            backup = await self._async_new_backup(slug, location)
            extra |= {"backup": backup, "location": location}
            if previous is not None and isinstance(previous.get("backup"), str):
                extra["previous_backup"] = previous["backup"]
        else:
            content = await self._async_supervisor_backup(slug)
        try:
            data = await self.hass.async_add_executor_job(
                pack,
                content,
                Path(self.hass.config.config_dir),
                paths,
                list(addon.exclude),
                addon.paths is not None,
            )
            manifest = await self.store.async_write(slug, data, extra)
        except BaseException:
            if backup is not None:
                await self._async_delete_backup(backup, location)
            raise
        # The backup before the previous one: no manifest names it any more.
        if previous is not None and isinstance(
            old := previous.get("previous_backup"), str
        ):
            await self._async_delete_backup(old, previous.get("location"))
        await self._async_remember(slug, manifest["id"])
        await self._async_note_in_index(slug)
        _LOGGER.debug(
            "Snapshot %s of add-on %s: %d bytes", manifest["id"], slug, manifest["size"]
        )

    async def _async_remember(self, slug: str, snapshot_id: str) -> None:
        self.local_snapshot[slug] = snapshot_id
        await self._marker.async_save({"snapshots": dict(self.local_snapshot)})

    async def _async_snapshot_loop(self) -> None:
        while self._leading:
            await asyncio.sleep(self.snapshot_interval)
            await self.async_snapshot_led()

    async def async_snapshot_led(self) -> None:
        """Snapshot every replicated add-on this node started as the leader."""
        for addon in list(self.addons):
            if not (self._leading and self._state_of(addon.slug).led):
                continue
            try:
                async with self._lock:
                    await self._async_snapshot(addon)
            except (TermSuperseded, NoTerm) as err:
                _LOGGER.debug("Snapshot of %s not written: %s", addon.slug, err)
                return
            except (SnapshotError, SupervisorError) as err:
                _LOGGER.warning("Could not snapshot add-on %s: %s", addon.slug, err)
            except HomeAssistantError as err:
                _LOGGER.warning(
                    "Could not store the snapshot of %s: %s", addon.slug, err
                )

    @callback
    def async_prepare_handover(self) -> None:
        """Home Assistant stops on this leader: hand the add-ons over later.

        Called before the node resigns, which would stop them at once,
        without a snapshot. They keep running until ``async_handover``, which
        runs in Core's stop stage (100 s for its tasks; the shutdown jobs
        before it have 20 s for everything). The lease is still held and
        renewed until the close event, so the snapshot is written in this
        node's term.
        """
        if self.active and self.started and self._leading:
            self._handover_pending = True

    async def async_handover(self) -> None:
        """Stop the add-ons and snapshot them stopped (the leader is shutting down).

        Bounded in time: a handover that does not finish leaves the add-ons
        stopped (or reported as maybe running) with the last periodic
        snapshot as the cluster's copy.
        """
        if not (self.active and self.started):
            return
        if not (self._leading or self._handover_pending):
            return
        self._handover_pending = False
        self._leading = False
        for task in (self._lead_task, self._snapshot_task):
            if task is not None and not task.done():
                task.cancel()
        led = [a for a in self.addons if self._state_of(a.slug).led]
        try:
            async with asyncio.timeout(_HANDOVER_TIMEOUT):
                async with self._lock:
                    for addon in reversed(self.addons):
                        await self._async_stop_confirmed(addon.slug)
                    for addon in led:
                        if addon.slug in self.maybe_running:
                            continue  # still running: its data may change
                        try:
                            await self._async_snapshot(addon)
                        except HomeAssistantError as err:
                            _LOGGER.warning(
                                "Could not snapshot add-on %s at the handover: %s",
                                addon.slug,
                                err,
                            )
        except TimeoutError:
            _LOGGER.warning("The add-on handover did not finish in time")
        for state in self._states.values():
            state.led = False
        await self.leases.async_publish_member()

    async def async_on_bucket_recreated(self) -> None:
        """Snapshot again: a recreated ``hass_storage`` lost the snapshots."""
        if self._leading:
            self._index_known = False
            await self._async_read_index()
            await self.async_snapshot_led()

    @callback
    def _note_exit_slugs(self) -> None:
        """Refresh the copy of what the watchdog stops (on the event loop).

        Called with every member key the node publishes (each renewal), so
        the copy is at most one renewal old.
        """
        running = self.maybe_running
        self._exit_slugs = tuple(
            addon.slug for addon in reversed(self.addons) if addon.slug in running
        )

    def stop_on_exit(self) -> None:
        """Stop managed add-ons when the watchdog ends the process.

        Runs in the watchdog's thread, which must end the process within
        milliseconds: the stop requests (Supervisor waits for each container
        to stop) are made by a detached helper process, which outlives this
        one. Best effort: if the helper cannot finish, the node stops them
        when it starts again, and the next leader waits ``addon_takeover_wait``
        for a node that may still run them.
        """
        slugs = self._exit_slugs
        if not slugs or not self.supervisor_token:
            return
        # Not waited for: it is to outlive this process.
        subprocess.Popen(  # pylint: disable=consider-using-with
            [sys.executable, "-I", "-c", _STOP_HELPER, SUPERVISOR_URL, *slugs],
            env={"SUPERVISOR_TOKEN": self.supervisor_token},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )

    # Repair issues

    def _report_issue(self, slug: str, reason: str) -> None:
        state = self._state_of(slug)
        if state.issue == reason:
            return
        state.issue = reason
        _LOGGER.warning(
            "Add-on %s is not started on cluster node %s: %s",
            slug,
            self.node_id,
            reason,
        )
        ir.async_create_issue(
            self.hass,
            ISSUE_DOMAIN,
            ISSUE_NOT_STARTED.format(slug=slug),
            is_fixable=False,
            severity=ir.IssueSeverity.ERROR,
            issue_domain=DOMAIN,
            translation_key="cluster_addon_not_started",
            translation_placeholders={
                "addon": slug,
                "node": self.node_id,
                "reason": reason,
            },
        )

    def _clear_issue(self, slug: str) -> None:
        state = self._state_of(slug)
        if state.issue is None:
            return
        state.issue = None
        ir.async_delete_issue(
            self.hass, ISSUE_DOMAIN, ISSUE_NOT_STARTED.format(slug=slug)
        )


def managed_addon_list(raw: list[Any]) -> tuple[list[ManagedAddon], bool]:
    """Return the named add-ons of ``managed_addons``, and whether ``all`` is in it."""
    result: list[ManagedAddon] = []
    manage_all = False
    for item in raw:
        if item == ADDON_ALL:
            manage_all = True
        elif isinstance(item, str):
            result.append(ManagedAddon(slug=item))
        else:
            paths = item.get(CONF_PATHS)
            result.append(
                ManagedAddon(
                    slug=item[CONF_SLUG],
                    paths=tuple(paths) if paths is not None else None,
                    exclude=tuple(item.get(CONF_EXCLUDE, ())),
                    replicate=item.get(CONF_REPLICATE, True),
                    seed_node=item.get(CONF_SEED_NODE),
                )
            )
    return result, manage_all
