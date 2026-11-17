"""Share bus events between nodes.

Automations run on the leader, but many of their triggers are events fired
by integrations that may run on another node (button presses such as
``zha_event``, ``mobile_app_notification_action``, ``tag_scanned``, device
triggers built on events). Every node publishes the events fired on its bus
to ``hass.event.<node>`` and fires the other nodes' events on its own bus
with ``EventOrigin.remote``.

* Events that describe a node rather than the house (``LOCAL_EVENT_TYPES``:
  service registrations, registry updates, start and stop, ...) are not
  shared, nor are state changes (the state bucket carries those), nor the
  ``<domain>_reloaded`` events every node fires when it reloads.
* Events Core fires as ``EventOrigin.remote`` (``POST /api/events``, mobile
  app webhooks) are shared too. Only the events this module fires itself
  are never published again, so events cannot loop.
* Delivery is at most once, like the bus itself: events fired while the
  nodes cannot reach each other are lost.
* The sender is the last token of the subject (``hass.event.<node>``),
  which NATS permissions restrict to that node's user; the ``origin`` in the
  body is not trusted. The user id of the context is dropped: users exist per
  node, and an id from another node names nobody here (or somebody else).
"""

import asyncio
from collections import deque
from contextlib import suppress
import logging
from typing import TYPE_CHECKING, Any

from homeassistant.const import MATCH_ALL
from homeassistant.core import (
    CALLBACK_TYPE,
    Context,
    Event,
    EventOrigin,
    HomeAssistant,
    callback,
)
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.json import json_bytes
from homeassistant.util.event_type import EventType
from homeassistant.util.json import json_loads

from .const import EVENT_SUBJECT, EVENT_SUBJECT_ALL, LOCAL_EVENT_TYPES, NODE_ID_RE
from .coordinator import ClusterCoordinator

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription

_LOGGER = logging.getLogger(__name__)

# Events waiting to be published while NATS is slow; the oldest are dropped.
MAX_QUEUED_EVENTS = 1000


class EventReplicator:
    """Publishes local bus events and fires remote ones."""

    def __init__(
        self, hass: HomeAssistant, node_id: str, coordinator: ClusterCoordinator
    ) -> None:
        """Initialize."""
        self.hass = hass
        self.node_id = node_id
        self.coordinator = coordinator
        self._subject = EVENT_SUBJECT.format(node_id=node_id)
        self._queue: deque[bytes] = deque(maxlen=MAX_QUEUED_EVENTS)
        self._wakeup = asyncio.Event()
        self._unsub: CALLBACK_TYPE | None = None
        self._sub: Subscription | None = None
        self._task: asyncio.Task[None] | None = None
        # Context ids of the events this module is firing right now. The bus
        # calls our (callback) listener synchronously from async_fire.
        self._firing: set[str] = set()

    async def async_start(self) -> None:
        """Subscribe to the other nodes and listen to the local bus."""
        if TYPE_CHECKING:
            assert self.coordinator.nc is not None
        self._sub = await self.coordinator.nc.subscribe(
            EVENT_SUBJECT_ALL, cb=self._async_on_message
        )
        self._task = self.hass.async_create_background_task(
            self._async_publish_loop(), f"cluster {self.node_id} event publisher"
        )
        self._unsub = self.hass.bus.async_listen(MATCH_ALL, self._async_on_event)

    async def async_stop(self) -> None:
        """Stop publishing and receiving."""
        if self._unsub is not None:
            self._unsub()
            self._unsub = None
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        if self._sub is not None:
            with suppress(Exception):
                await self._sub.unsubscribe()
            self._sub = None

    @callback
    def _async_on_event(self, event: Event) -> None:
        if not is_shared(event.event_type) or (
            event.origin is EventOrigin.remote and event.context.id in self._firing
        ):
            return
        try:
            payload = json_bytes(
                {
                    "origin": self.node_id,
                    "event_type": event.event_type,
                    "data": event.data,
                    "time_fired": event.time_fired_timestamp,
                    "context": {
                        "id": event.context.id,
                        "parent_id": event.context.parent_id,
                    },
                }
            )
        except HomeAssistantError, TypeError, ValueError:
            _LOGGER.debug("Not sharing %s: data is not JSON", event.event_type)
            return
        if len(payload) > self.coordinator.max_payload:
            _LOGGER.debug("Not sharing %s: too large", event.event_type)
            return
        self._queue.append(payload)
        self._wakeup.set()

    async def _async_publish_loop(self) -> None:
        while True:
            await self._wakeup.wait()
            self._wakeup.clear()
            nc = self.coordinator.nc
            while self._queue:
                if nc is None or not self.coordinator.connected:
                    self._queue.clear()  # at most once: the bus doesn't queue
                    break
                payload = self._queue.popleft()
                try:
                    await nc.publish(self._subject, payload)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.debug("Could not share an event: %s", err)

    async def _async_on_message(self, msg: Msg) -> None:
        if msg.subject == self._subject:
            return
        origin = msg.subject.removeprefix(EVENT_SUBJECT_ALL[:-1])
        if origin == self.node_id or not NODE_ID_RE.match(origin):
            return
        try:
            payload: dict[str, Any] = json_loads(msg.data)  # type: ignore[assignment]
            if payload.get("origin") not in (None, origin):
                _LOGGER.warning(
                    "Ignoring an event on %s that claims to come from %s",
                    msg.subject,
                    payload.get("origin"),
                )
                return
            event_type: str = payload["event_type"]
            ctx = payload.get("context") or {}
            context = Context(parent_id=ctx.get("parent_id"), id=ctx.get("id"))
            data = payload.get("data")
            time_fired = payload.get("time_fired")
        except ValueError, KeyError, TypeError:
            _LOGGER.warning("Ignoring a malformed shared event on %s", msg.subject)
            return
        if not is_shared(event_type) or not isinstance(data, dict):
            return
        self._firing.add(context.id)
        try:
            self.hass.bus.async_fire(
                event_type,
                data,
                origin=EventOrigin.remote,
                context=context,
                time_fired=time_fired if isinstance(time_fired, float) else None,
            )
        except HomeAssistantError as err:  # e.g. an event type that is too long
            _LOGGER.debug("Could not fire shared event %s: %s", event_type, err)
        finally:
            self._firing.discard(context.id)


def is_shared(event_type: EventType[Any] | str) -> bool:
    """Return True for an event type that is shared with the other nodes."""
    return event_type not in LOCAL_EVENT_TYPES and not str(event_type).endswith(
        "_reloaded"
    )
