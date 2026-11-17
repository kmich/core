"""What a node remembers to recover from lost cluster data, and repair issues.

``hass_storage`` can lose records: a bucket that is recreated loses every
tombstone, and a leader can crash between an integration's clean-up and the
tombstone. Each node therefore keeps, in a private store that is never
replicated:

* the ids of the entries it knows from the cluster (published by a leader,
  or by itself as the leader), as opposed to entries made on this node only;
* a log of the latest removals of replicated entries;
* whether it has ever seen a cluster inventory, so that a missing one means
  lost data and not a first start.
"""

from collections.abc import Callable, Iterable
import logging
from time import monotonic, time
from typing import Any

from homeassistant.config_entries import SOURCE_REAUTH, ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers import (
    config_entry_oauth2_flow,
    issue_registry as ir,
    storage,
)

from .const import DOMAIN, PROTOCOL_VERSION

_LOGGER = logging.getLogger(__name__)

STORE_KEY = "cluster.durability"
STORE_VERSION = 1
_SAVE_DELAY = 1.0

# Removals kept in the log, newest last.
MAX_REMOVALS = 256

# How long after a takeover a reauthentication counts as "token lost".
TOKEN_LOST_WINDOW = 600.0

# The issue texts live with Home Assistant core's own translations.
ISSUE_DOMAIN = "homeassistant"
ISSUE_DATA_LOST = "storage_data_lost"
ISSUE_TOKEN_LOST = "token_lost_{entry_id}"
ISSUE_MISSING_CREDENTIAL = "missing_credential_{entry_id}"


class NodeMemory:
    """The private record of what this node knows about the cluster's entries."""

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize; ``async_load`` reads the store."""
        self.hass = hass
        self._store = storage.Store[dict[str, Any]](
            hass, STORE_VERSION, STORE_KEY, private=True
        )
        self.known: set[str] = set()
        self.removed: list[tuple[str, float]] = []
        self.seen_inventory = False
        # Announced removals that have no tombstone yet, by entry id.
        self.intents: dict[str, dict[str, Any]] = {}

    async def async_load(self) -> None:
        """Read what this node remembered."""
        data = await self._store.async_load() or {}
        self.known = {i for i in data.get("known", []) if isinstance(i, str)}
        self.removed = [
            (item[0], float(item[1]))
            for item in data.get("removed", [])
            if isinstance(item, list)
            and len(item) == 2
            and isinstance(item[0], str)
            and isinstance(item[1], (int, float))
        ][-MAX_REMOVALS:]
        self.seen_inventory = bool(data.get("seen_inventory"))
        self.intents = {
            k: v
            for k, v in (data.get("intents") or {}).items()
            if isinstance(k, str) and isinstance(v, dict)
        }

    @callback
    def _async_save(self) -> None:
        self._store.async_delay_save(self._data, _SAVE_DELAY)

    def _data(self) -> dict[str, Any]:
        return {
            "known": sorted(self.known),
            "removed": [list(item) for item in self.removed],
            "seen_inventory": self.seen_inventory,
            "intents": self.intents,
        }

    async def async_note_intent(self, entry_id: str, payload: dict[str, Any]) -> None:
        """Remember an announced removal now: its clean-up may not finish."""
        self.intents[entry_id] = payload
        await self._store.async_save(self._data())

    @callback
    def async_clear_intent(self, entry_id: str) -> None:
        """The removal is complete (its tombstone was published)."""
        if self.intents.pop(entry_id, None) is not None:
            self._async_save()

    async def async_mark_known(self, entry_id: str) -> None:
        """The entry is part of the cluster (a leader published it).

        Saved at once: a crash before the save would make the entry look like
        one made on this node only, and a removal could not reach it.
        """
        if entry_id not in self.known:
            self.known.add(entry_id)
            await self._store.async_save(self._data())

    @callback
    def async_note_removed(self, entry_id: str) -> None:
        """The replicated entry was removed (here, or by a leader)."""
        self.known.discard(entry_id)
        self.intents.pop(entry_id, None)
        if any(item[0] == entry_id for item in self.removed):
            return
        self.removed.append((entry_id, time()))
        del self.removed[:-MAX_REMOVALS]
        self._async_save()

    @callback
    def async_mark_seen_inventory(self) -> None:
        """An inventory existed in the bucket."""
        if not self.seen_inventory:
            self.seen_inventory = True
            self._async_save()

    def removed_ids(self) -> set[str]:
        """Ids of the removals in the log."""
        return {item[0] for item in self.removed}


def build_inventory(
    node_id: str, term: int, entry_ids: Iterable[str], memory: NodeMemory
) -> dict[str, Any]:
    """The inventory record a leader publishes."""
    return {
        "protocol": PROTOCOL_VERSION,
        "origin": node_id,
        "term": term,
        "ts": time(),
        "entries": sorted(entry_ids),
        "removed": [list(item) for item in memory.removed],
    }


def parse_inventory(payload: Any) -> tuple[set[str], set[str]] | None:
    """Return the entry ids and the removed ids of an inventory record."""
    if not isinstance(payload, dict):
        return None
    entries = payload.get("entries")
    removed = payload.get("removed")
    if not isinstance(entries, list) or not isinstance(removed, list):
        return None
    return (
        {i for i in entries if isinstance(i, str)},
        {
            item[0]
            for item in removed
            if isinstance(item, list) and item and isinstance(item[0], str)
        },
    )


@callback
def async_raise_data_lost(hass: HomeAssistant) -> None:
    """Ask the user to check for entries another node removed."""
    ir.async_create_issue(
        hass,
        ISSUE_DOMAIN,
        ISSUE_DATA_LOST,
        is_fixable=False,
        is_persistent=True,
        severity=ir.IssueSeverity.WARNING,
        issue_domain=DOMAIN,
        translation_key="cluster_storage_data_lost",
    )


@callback
def async_clear_data_lost(hass: HomeAssistant) -> None:
    """The cluster's inventory is back."""
    ir.async_delete_issue(hass, ISSUE_DOMAIN, ISSUE_DATA_LOST)


async def async_check_credentials(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Raise an issue if an OAuth entry's application credential is missing here.

    Application credentials are configured on every node, not replicated. A
    node that takes over without the one an entry uses cannot refresh its
    token.
    """
    if not isinstance(implementation := entry.data.get("auth_implementation"), str):
        return
    issue_id = ISSUE_MISSING_CREDENTIAL.format(entry_id=entry.entry_id)
    try:
        available = await config_entry_oauth2_flow.async_get_implementations(
            hass, entry.domain
        )
    except Exception:  # noqa: BLE001 - a provider that is down is not "missing"
        return
    if implementation in available:
        ir.async_delete_issue(hass, ISSUE_DOMAIN, issue_id)
        return
    _LOGGER.warning(
        "%s (%s) uses the application credential %s, which is not configured"
        " on this node",
        entry.title,
        entry.domain,
        implementation,
    )
    ir.async_create_issue(
        hass,
        ISSUE_DOMAIN,
        issue_id,
        is_fixable=False,
        severity=ir.IssueSeverity.ERROR,
        issue_domain=DOMAIN,
        translation_key="cluster_missing_credential",
        translation_placeholders={
            "title": entry.title,
            "domain": entry.domain,
            "credential": implementation,
        },
    )


class TakeoverWatch:
    """Explain a reauthentication that starts soon after a takeover.

    A token the old leader refreshed just before it failed may not have been
    published; the provider then refuses the copy this node has.
    """

    def __init__(
        self, hass: HomeAssistant, replicated: Callable[[ConfigEntry], bool]
    ) -> None:
        """Initialize."""
        self.hass = hass
        self._replicated = replicated
        self._until = 0.0
        self._previous: str | None = None
        self._unsub: CALLBACK_TYPE | None = None

    @callback
    def async_took_over(self, previous: str | None) -> None:
        """This node took over from ``previous`` (None if unknown)."""
        self._until = monotonic() + TOKEN_LOST_WINDOW
        self._previous = previous
        if self._unsub is None:
            self._unsub = self.hass.config_entries.flow.async_subscribe_flow(
                self._async_on_flow
            )

    @callback
    def async_stop(self) -> None:
        """Stop watching."""
        if self._unsub is not None:
            self._unsub()
            self._unsub = None

    @callback
    def _async_on_flow(self, event: str, flow_id: str) -> None:
        if monotonic() > self._until:
            self.async_stop()
            return
        if event != "added":
            return
        try:
            flow = self.hass.config_entries.flow.async_get(flow_id)
        except Exception:  # noqa: BLE001 - finished meanwhile
            return
        context = flow.get("context") or {}
        if context.get("source") != SOURCE_REAUTH:
            return
        if (entry_id := context.get("entry_id")) is None or (
            entry := self.hass.config_entries.async_get_entry(entry_id)
        ) is None:
            return
        if not self._replicated(entry):
            return
        _LOGGER.warning(
            "%s (%s) needs reauthentication soon after this node took over;"
            " its token may have been refreshed on %s just before it failed",
            entry.title,
            entry.domain,
            self._previous or "the previous leader",
        )
        ir.async_create_issue(
            self.hass,
            ISSUE_DOMAIN,
            ISSUE_TOKEN_LOST.format(entry_id=entry_id),
            is_fixable=False,
            severity=ir.IssueSeverity.WARNING,
            issue_domain=DOMAIN,
            translation_key="cluster_token_lost",
            translation_placeholders={
                "title": entry.title,
                "previous": self._previous or "the previous leader",
            },
        )
