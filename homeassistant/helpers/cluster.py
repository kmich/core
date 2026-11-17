"""The cluster role of this Home Assistant instance.

Several instances can form a cluster (`homeassistant/cluster`): each
config entry runs on one of them, and the state machine and events are
shared. Some work has no config entry and must still happen on exactly one
instance, or it happens once per instance: automations and scripts. That
instance is the leader.

Without a cluster this instance is the leader, so code that
checks `async_is_leader` behaves as before on a single instance.
"""

import asyncio
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
import logging
from time import monotonic
from typing import TYPE_CHECKING, Any

from homeassistant import loader
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.util.hass_dict import HassKey

from .storage import Store

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry, ConfigEntryPlacement

_LOGGER = logging.getLogger(__name__)

DATA_CLUSTER_ROLE: HassKey[ClusterRole] = HassKey("cluster_role")
DATA_FAIL_CLOSED: HassKey[CALLBACK_TYPE] = HassKey("cluster_fail_closed")

# Config entries of these domains run on every instance of a cluster.
EVERY_INSTANCE_DOMAINS = frozenset(
    {
        "backup",
        "cluster",
        "hassio",
        "homeassistant",
        "logger",
        "persistent_notification",
        "system_log",
    }
)


@callback
def async_entry_runs_on_every_instance(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Return True for a config entry that each instance of a cluster runs.

    Per-instance infrastructure: the domains above, and integrations of type
    ``system`` (analytics, cloud, go2rtc, ...), for which every instance
    creates its own config entry. That type also covers ``alexa`` and
    ``google_assistant`` (each instance's own YAML), legacy notify platforms
    and stubs of removed integrations; the cluster README lists what it
    means for a cluster. Every other config entry runs on one instance at a
    time.
    """
    if entry.domain in EVERY_INSTANCE_DOMAINS:
        return True
    try:
        integration = loader.async_get_loaded_integration(hass, entry.domain)
    except loader.IntegrationNotLoaded:
        return False
    return integration.integration_type == "system"


class FailClosedPlacement:
    """Placement used until a configured cluster took over.

    Bootstrap installs it as soon as the configuration names a cluster, before
    any integration is resolved or set up. Without it, config entries would
    start here while other instances may run them too, for example when the
    cluster cannot be loaded. Entries that run on every instance
    are not affected.
    """

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize."""
        self.hass = hass

    async def async_place_entry(self, entry: ConfigEntry) -> str | None:
        """Keep clustered entries unloaded."""
        return self.async_refuse_removal(entry)

    @callback
    def async_refuse_removal(self, entry: ConfigEntry) -> str | None:
        """Refuse to remove clustered entries: another instance may run them."""
        if async_entry_runs_on_every_instance(self.hass, entry):
            return None
        return "The cluster integration is not set up"

    @callback
    def async_refuse_change(self, entry: ConfigEntry, change: str) -> str | None:
        """Refuse changes to clustered entries: another instance may run them."""
        return self.async_refuse_removal(entry)

    async def async_before_remove(self, entry: ConfigEntry) -> None:
        """Nothing to record: only entries of every instance are removed."""


@callback
def async_install_fail_closed(hass: HomeAssistant) -> None:
    """Keep clustered config entries unloaded until a cluster takes over.

    For bootstrap, as soon as the configuration names a cluster. The cluster
    package replaces it with ``async_take_over_placement``.
    """
    if DATA_FAIL_CLOSED in hass.data:
        return
    hass.data[DATA_FAIL_CLOSED] = hass.config_entries.async_set_placement(
        FailClosedPlacement(hass)
    )
    async_set_leader(hass, False)


@callback
def async_take_over_placement(
    hass: HomeAssistant, placement: ConfigEntryPlacement
) -> CALLBACK_TYPE:
    """Replace the fail-closed placement (if any) with ``placement``.

    For cluster integrations. Leadership is set to False first, so nothing
    that checks it runs before this instance is elected. Returns a callback
    that removes ``placement``.
    """
    async_set_leader(hass, False)
    if (remove := hass.data.pop(DATA_FAIL_CLOSED, None)) is not None:
        remove()
    return hass.config_entries.async_set_placement(placement)


def _marker_store(hass: HomeAssistant) -> Store[dict[str, bool]]:
    return Store(hass, 1, "cluster", private=True)


#: Top-level configuration key of the cluster (see ``homeassistant/cluster``).
CLUSTER_CONFIG_KEY = "cluster"
#: What the cluster needs and no integration declares; bootstrap installs it
#: when ``cluster:`` is configured. Keep equal to the ``cluster`` extra of
#: ``pyproject.toml``.
CLUSTER_REQUIREMENTS = ("nats-py[nkeys]==2.16.0",)


#: Repair issues of a cluster that cannot work (texts: ``homeassistant``
#: translations, ``cluster_*``). Raised from here, not from the cluster
#: package, because the package may be the thing that cannot be imported.
ISSUE_DOMAIN = "homeassistant"
ISSUE_SETUP_FAILED = "cluster_setup_failed"
ISSUE_CANNOT_TAKE_OVER = "cluster_cannot_take_over"
ISSUE_CONNECTION_REFUSED = "cluster_connection_refused"
ISSUE_VERSION_MISMATCH = "cluster_version_mismatch"


@callback
def async_report_setup_failed(hass: HomeAssistant, reason: str) -> None:
    """Tell the user that the cluster could not be set up on this instance.

    Config entries that run on one instance at a time stay unloaded.
    """
    from . import issue_registry as ir  # noqa: PLC0415

    ir.async_create_issue(
        hass,
        ISSUE_DOMAIN,
        ISSUE_SETUP_FAILED,
        is_fixable=False,
        severity=ir.IssueSeverity.ERROR,
        issue_domain=CLUSTER_CONFIG_KEY,
        translation_key=ISSUE_SETUP_FAILED,
        translation_placeholders={"reason": reason},
    )


@callback
def async_clear_setup_failed(hass: HomeAssistant) -> None:
    """The cluster was set up (or this instance is not a member any more)."""
    from . import issue_registry as ir  # noqa: PLC0415

    ir.async_delete_issue(hass, ISSUE_DOMAIN, ISSUE_SETUP_FAILED)


@callback
def async_report_connection_refused(hass: HomeAssistant, reason: str) -> None:
    """Tell the user that the NATS server or this node's setup was refused.

    Strict mode refuses a server without TLS or a user that may publish as
    any node; a certificate or credentials can be rejected. The node keeps
    trying, so fixing the server side is enough.
    """
    from . import issue_registry as ir  # noqa: PLC0415

    ir.async_create_issue(
        hass,
        ISSUE_DOMAIN,
        ISSUE_CONNECTION_REFUSED,
        is_fixable=False,
        severity=ir.IssueSeverity.ERROR,
        issue_domain=CLUSTER_CONFIG_KEY,
        translation_key=ISSUE_CONNECTION_REFUSED,
        translation_placeholders={"reason": reason},
    )


@callback
def async_clear_connection_refused(hass: HomeAssistant) -> None:
    """A connection to NATS was accepted (or this node is not a member any more)."""
    from . import issue_registry as ir  # noqa: PLC0415

    ir.async_delete_issue(hass, ISSUE_DOMAIN, ISSUE_CONNECTION_REFUSED)


@callback
def async_report_cannot_take_over(hass: HomeAssistant, node: str, reason: str) -> None:
    """Tell the user that this node cannot become the cluster leader."""
    from . import issue_registry as ir  # noqa: PLC0415

    ir.async_create_issue(
        hass,
        ISSUE_DOMAIN,
        ISSUE_CANNOT_TAKE_OVER,
        is_fixable=False,
        severity=ir.IssueSeverity.ERROR,
        issue_domain=CLUSTER_CONFIG_KEY,
        translation_key=ISSUE_CANNOT_TAKE_OVER,
        translation_placeholders={"node": node, "reason": reason},
    )


@callback
def async_report_version_mismatch(hass: HomeAssistant, versions: str) -> None:
    """Tell the user that cluster nodes run different Home Assistant versions."""
    from . import issue_registry as ir  # noqa: PLC0415

    ir.async_create_issue(
        hass,
        ISSUE_DOMAIN,
        ISSUE_VERSION_MISMATCH,
        is_fixable=False,
        severity=ir.IssueSeverity.WARNING,
        issue_domain=CLUSTER_CONFIG_KEY,
        translation_key=ISSUE_VERSION_MISMATCH,
        translation_placeholders={"versions": versions},
    )


@callback
def async_clear_version_mismatch(hass: HomeAssistant) -> None:
    """All cluster nodes run the same version again."""
    from . import issue_registry as ir  # noqa: PLC0415

    ir.async_delete_issue(hass, ISSUE_DOMAIN, ISSUE_VERSION_MISMATCH)


@callback
def async_clear_cannot_take_over(hass: HomeAssistant) -> None:
    """This node led successfully (or the reason is gone)."""
    from . import issue_registry as ir  # noqa: PLC0415

    ir.async_delete_issue(hass, ISSUE_DOMAIN, ISSUE_CANNOT_TAKE_OVER)


async def async_save_membership_marker(hass: HomeAssistant) -> None:
    """Remember that this instance's configuration names a cluster.

    Recovery mode reads it when the configuration cannot be loaded. It is in a
    private store, so it is not replicated to other instances.
    """
    await _marker_store(hass).async_save({"member": True})


async def async_has_membership_marker(hass: HomeAssistant) -> bool:
    """Return True if this instance started as a cluster member before."""
    return await _marker_store(hass).async_load() is not None


async def async_clear_membership_marker(hass: HomeAssistant) -> None:
    """Forget the marker: the readable configuration no longer names a cluster."""
    if await async_has_membership_marker(hass):
        await _marker_store(hass).async_remove()


@dataclass(slots=True)
class ClusterRole:
    """Leadership of this instance and who wants to know when it changes."""

    leader: bool = True
    valid_until: float | None = None
    # True once a cluster integration (or the fail-closed placement) set the
    # leadership; until then this instance runs on its own.
    clustered: bool = False
    every_instance_domains: frozenset[str] = frozenset()
    listeners: list[Callable[[bool], None]] = field(default_factory=list)
    # Work that a change of leadership started (stopping automation and
    # script runs, taking down platforms) and that has not finished yet.
    transitions: set[asyncio.Future[Any]] = field(default_factory=set)
    # Work that failed or was cancelled before it finished. What it was
    # stopping may still run, so it counts as pending until the process ends.
    failures: list[str] = field(default_factory=list)


@callback
def _async_get_role(hass: HomeAssistant) -> ClusterRole:
    if (role := hass.data.get(DATA_CLUSTER_ROLE)) is None:
        role = hass.data[DATA_CLUSTER_ROLE] = ClusterRole()
    return role


@callback
def async_is_leader(hass: HomeAssistant) -> bool:
    """Return True if this instance runs the work that must run only once.

    Always True without a cluster integration. A leader whose deadline
    (``async_set_leader_deadline``) has passed is not a leader any more, even
    if nothing called ``async_set_leader`` yet, for example because the event
    loop was stalled.
    """
    if (role := hass.data.get(DATA_CLUSTER_ROLE)) is None:
        return True
    if not role.leader:
        return False
    return role.valid_until is None or monotonic() < role.valid_until


@callback
def async_is_clustered(hass: HomeAssistant) -> bool:
    """Return True if this instance is a member of a cluster.

    Without a cluster integration nothing needs to track leadership, so code
    can skip its cluster bookkeeping.
    """
    return (role := hass.data.get(DATA_CLUSTER_ROLE)) is not None and role.clustered


@callback
def async_set_every_instance_domains(
    hass: HomeAssistant, domains: Iterable[str]
) -> None:
    """Set the domains that run on every instance. For cluster integrations."""
    _async_get_role(hass).every_instance_domains = frozenset(domains)


@callback
def async_domain_runs_on_every_instance(hass: HomeAssistant, domain: str) -> bool:
    """Return True if ``domain`` runs on every instance of a cluster.

    That is a domain of ``EVERY_INSTANCE_DOMAINS``, one the cluster integration
    configured, or an integration of type ``system``. An integration that is
    not loaded is not one of them.
    """
    if domain in EVERY_INSTANCE_DOMAINS:
        return True
    if (
        role := hass.data.get(DATA_CLUSTER_ROLE)
    ) is not None and domain in role.every_instance_domains:
        return True
    try:
        integration = loader.async_get_loaded_integration(hass, domain)
    except loader.IntegrationNotLoaded:
        return False
    return integration.integration_type == "system"


@callback
def async_set_leader_deadline(hass: HomeAssistant, deadline: float | None) -> None:
    """Limit leadership to ``time.monotonic()`` values below ``deadline``.

    For cluster integrations. After the deadline ``async_is_leader`` returns
    False until the deadline is moved forward or removed (``None``).
    Listeners are not called when the deadline passes: only
    ``async_set_leader`` calls them.
    """
    _async_get_role(hass).valid_until = deadline


@callback
def async_set_leader(hass: HomeAssistant, leader: bool) -> None:
    """Change whether this instance is the leader. For cluster integrations.

    A cluster integration sets this to False as soon as it is set up, before
    anything checks it, and to True when this instance is elected. Listeners
    are called synchronously, in the order they were added.
    """
    role = _async_get_role(hass)
    role.clustered = True
    if role.leader is leader:
        return
    role.leader = leader
    for listener in role.listeners.copy():
        try:
            listener(leader)
        except Exception:
            _LOGGER.exception("Error in cluster leadership listener")
            if not leader:
                # What the listener should have stopped may still run.
                role.failures.append(f"leadership listener {listener!r} failed")


@callback
def async_track_leadership(
    hass: HomeAssistant, listener: Callable[[bool], None]
) -> CALLBACK_TYPE:
    """Call ``listener`` with the new value whenever leadership changes.

    The listener must be a callback; schedule a task for anything slower.
    Returns a callback that removes the listener.
    """
    listeners = _async_get_role(hass).listeners
    listeners.append(listener)

    @callback
    def _remove() -> None:
        with suppress(ValueError):
            listeners.remove(listener)

    return _remove


@callback
def async_add_transition_task(
    hass: HomeAssistant, task: asyncio.Future[Any], what: str = "leadership change"
) -> None:
    """Track work a change of leadership started, until it is done.

    A leadership listener that stops work in a task (an automation stopping
    its runs, a component taking down its platforms) registers the task here.
    A cluster integration must not let another instance take over before
    ``async_transitions_pending`` returns False. A task that raises or is
    cancelled did not finish what it stopped: it stays pending for good.
    """
    role = _async_get_role(hass)

    def _done(done: asyncio.Future[Any]) -> None:
        role.transitions.discard(done)
        if done.cancelled():
            role.failures.append(f"{what} was cancelled")
        elif (err := done.exception()) is not None:
            _LOGGER.error("Cluster work failed: %s", what, exc_info=err)
            role.failures.append(f"{what} failed: {err!r}")

    if task.done():
        _done(task)
        return
    role.transitions.add(task)
    task.add_done_callback(_done)


@contextmanager
def async_track_work(hass: HomeAssistant, what: str) -> Generator[None]:
    """Track work that must finish before this instance gives up leadership.

    For work that is no longer visible to the usual checks while it runs, such
    as the clean-up of a config entry that was already removed. Raising (or
    being cancelled) inside the block counts as failed work. Free on an
    instance that is not part of a cluster.
    """
    if not async_is_clustered(hass):
        yield
        return
    work: asyncio.Future[None] = hass.loop.create_future()
    role = _async_get_role(hass)
    role.transitions.add(work)
    try:
        yield
    except BaseException as err:
        role.failures.append(f"{what} failed: {err!r}")
        raise
    finally:
        role.transitions.discard(work)
        work.cancel()


@callback
def async_record_failure(hass: HomeAssistant, what: str) -> None:
    """Record that work whose success matters to a cluster did not succeed."""
    if async_is_clustered(hass):
        _async_get_role(hass).failures.append(what)


@callback
def async_transition_failures(hass: HomeAssistant) -> list[str]:
    """Return what failed to stop; empty while everything finished cleanly."""
    if (role := hass.data.get(DATA_CLUSTER_ROLE)) is None:
        return []
    return list(role.failures)


@callback
def async_transitions_pending(hass: HomeAssistant) -> bool:
    """Return True while work started by a change of leadership may still run.

    That is work that has not finished and work that failed.
    """
    return (role := hass.data.get(DATA_CLUSTER_ROLE)) is not None and bool(
        role.transitions or role.failures
    )


async def async_wait_transitions(
    hass: HomeAssistant, timeout: float | None = None
) -> bool:
    """Wait until the work started by changes of leadership is done.

    Includes work that is started while waiting. Returns False if ``timeout``
    passed or the work failed; its exceptions are logged, not raised.
    """
    if (role := hass.data.get(DATA_CLUSTER_ROLE)) is None:
        return True
    try:
        async with asyncio.timeout(timeout):
            while role.transitions:
                await asyncio.wait(set(role.transitions))
    except TimeoutError:
        return False
    return not role.failures
