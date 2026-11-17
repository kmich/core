"""Route service calls to the cluster leader or execute locally.

In Single-Leader mode:
* The Leader executes all service calls locally via ``_async_call_local``.
  For configuration reloads and entity toggles of leader domains
  (automations and scripts), the Leader also broadcasts best-effort to Standby
  nodes so their local entities stay updated for failover.
* Standby nodes execute node-local services (e.g. logger, recorder, frontend)
  locally, and forward all other service calls unsplit to the Leader.
"""

import asyncio
from collections.abc import Coroutine, Iterable
from contextlib import suppress
import logging
from typing import TYPE_CHECKING, Any

import probatio

from homeassistant.auth.const import GROUP_ID_ADMIN, GROUP_ID_READ_ONLY, GROUP_ID_USER
from homeassistant.auth.models import User
from homeassistant.auth.permissions.const import POLICY_CONTROL
from homeassistant.const import ENTITY_MATCH_ALL
from homeassistant.core import (
    CALLBACK_TYPE,
    DOMAIN as HOMEASSISTANT_DOMAIN,
    Context,
    HomeAssistant,
    ServiceResponse,
)
from homeassistant.exceptions import (
    HomeAssistantError,
    ServiceNotFound,
    ServiceValidationError,
    Unauthorized,
    UnknownUser,
)
from homeassistant.helpers.json import json_bytes
from homeassistant.helpers.target import (
    TargetSelection,
    async_extract_referenced_entity_ids,
)
from homeassistant.util.json import json_loads

from .const import (
    LEADER_DOMAIN_BROADCAST_SERVICES,
    LOCAL_SERVICE_DOMAINS,
    MAX_INBOUND_RPC,
    NODE_ID_RE,
    REMOTE_USER_NAME,
    ROUTED_HOMEASSISTANT_SERVICES,
    RPC_SUBJECT,
    RPC_SUBJECT_TO,
)
from .coordinator import ClusterCoordinator
from .lease_manager import LeaseManager

if TYPE_CHECKING:
    from nats.aio.msg import Msg
    from nats.aio.subscription import Subscription

    from .state_replicator import StateReplicator

_LOGGER = logging.getLogger(__name__)

ROLE_ADMIN = "admin"
ROLE_USER = "user"
ROLE_READ_ONLY = "read_only"
# A call without a user (an integration's own call), as on a single
# instance. Accepted only from a peer that NATS authenticated by subject.
ROLE_SYSTEM = "system"

_ROLE_RANK = {ROLE_READ_ONLY: 0, ROLE_USER: 1, ROLE_ADMIN: 2}

type _CallArgs = tuple[
    str, str, dict[str, Any] | None, bool, Context | None, dict[str, Any] | None, bool
]


class RemoteServiceError(HomeAssistantError):
    """Raised when a remote node could not be reached or did not answer."""


class ServiceRouter:
    """Executes service calls locally on the leader or forwards them to the leader."""

    def __init__(
        self,
        hass: HomeAssistant,
        node_id: str,
        coordinator: ClusterCoordinator,
        lease_manager: LeaseManager,
        replicator: StateReplicator,
        *,
        rpc_timeout: float,
    ) -> None:
        """Initialize the router."""
        self.hass = hass
        self.node_id = node_id
        self.coordinator = coordinator
        self.leases = lease_manager
        self.replicator = replicator
        self.rpc_timeout = rpc_timeout
        self._sub: Subscription | None = None
        self._unregister: CALLBACK_TYPE | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        # Forwarded calls this node is running for another node.
        self._rpc_tasks: set[asyncio.Task[None]] = set()
        # The services of forwarded non-blocking calls: the call is answered
        # when the service is started, and it runs on for as long as it takes.
        self._service_tasks: set[asyncio.Task[Any]] = set()
        # Calls this node runs as the leader (made here: the UI, the API,
        # an automation): work of the leader too, stopped when it ends.
        self._leader_tasks: set[asyncio.Task[Any]] = set()
        self._inbound = 0
        self._remote_user_ids: dict[str, str] = {}
        self._remote_user_lock = asyncio.Lock()

    async def async_start(self) -> None:
        """Subscribe to this node's RPC subject and register as router."""
        if TYPE_CHECKING:
            assert self.coordinator.nc is not None
        self._sub = await self.coordinator.nc.subscribe(
            RPC_SUBJECT_TO.format(node_id=self.node_id), cb=self._async_on_rpc
        )
        self._unregister = self.hass.services.async_set_router(self)
        self.leases.add_work_tracker(self.busy, self.async_drain)

    def busy(self) -> bool:
        """Return True while a call runs on this node as the leader.

        A forwarded call, or one made here while this node leads: work of
        the leader, so the lease is not released cleanly meanwhile.
        """
        return bool(self._rpc_tasks or self._service_tasks or self._leader_tasks)

    async def async_drain(self, timeout: float) -> None:
        """Let the leader's calls finish for ``timeout`` seconds, then cancel them."""
        tasks = self._rpc_tasks | self._service_tasks | self._leader_tasks
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            _LOGGER.warning(
                "Cancelled %s service calls of the leader that did not finish within"
                " %.1fs of the end of leadership",
                len(pending),
                timeout,
            )
            await asyncio.wait(pending, timeout=min(timeout, 1.0))

    async def async_stop(self) -> None:
        """Unregister, unsubscribe and stop the forwarded calls."""
        if self._unregister is not None:
            self._unregister()
            self._unregister = None
        if self._sub is not None:
            with suppress(Exception):
                await self._sub.unsubscribe()
            self._sub = None
        await self.async_drain(self.rpc_timeout)

    async def _local(
        self,
        domain: str,
        service: str,
        service_data: dict[str, Any] | None,
        blocking: bool,
        context: Context | None,
        target: dict[str, Any] | None,
        return_response: bool,
        track: set[asyncio.Task[Any]] | None = None,
    ) -> ServiceResponse:
        return await self.hass.services._async_call_local(  # noqa: SLF001
            domain,
            service,
            service_data,
            blocking,
            context,
            target,
            return_response,
            track,
        )

    def _other_nodes(self) -> set[str]:
        return {n for n in self.leases.members if n != self.node_id} | {
            n for n in self.leases.known_nodes if n != self.node_id
        }

    def _is_local_only(self, domain: str, service: str) -> bool:
        domain_lower = domain.lower()
        if domain_lower in self.leases.exclude_domains:
            return True
        if domain_lower in LOCAL_SERVICE_DOMAINS and not (
            domain_lower == HOMEASSISTANT_DOMAIN
            and service.lower() in ROUTED_HOMEASSISTANT_SERVICES
        ):
            return True
        return False

    async def _async_caller(self, context: Context) -> tuple[User | None, str]:
        if context.user_id is None:
            return None, ROLE_ADMIN
        if (user := await self.hass.auth.async_get_user(context.user_id)) is None:
            raise UnknownUser(context=context)
        return user, _user_role(user)

    async def _async_authorize(
        self, context: Context, entity_ids: Iterable[str]
    ) -> str:
        user, role = await self._async_caller(context)
        if user is None or role == ROLE_ADMIN:
            return role
        for entity_id in entity_ids:
            if not user.permissions.check_entity(entity_id, POLICY_CONTROL):
                raise Unauthorized(
                    context=context, entity_id=entity_id, permission=POLICY_CONTROL
                )
        return role

    async def async_route_service_call(
        self,
        domain: str,
        service: str,
        service_data: dict[str, Any] | None,
        blocking: bool,
        context: Context | None,
        target: dict[str, Any] | None,
        return_response: bool,
    ) -> ServiceResponse:
        """Execute or route a service call."""
        args: _CallArgs = (
            domain,
            service,
            service_data,
            blocking,
            context,
            target,
            return_response,
        )

        # 1. On the Leader: execute locally with zero target splitting
        if self.leases.is_leader:
            result = await self._async_run_as_leader(args)
            broadcast = LEADER_DOMAIN_BROADCAST_SERVICES.get(
                domain.lower(), frozenset()
            )
            if service.lower() in broadcast and self._other_nodes():
                context = context or Context()
                _, role = await self._async_caller(context)
                self._broadcast(args, role=role, remote_errors_raise=False)
            return result

        # 2. On Standby: local-only services execute locally
        if self._is_local_only(domain, service):
            return await self._local(*args)

        # 3. On Standby: handle broadcast leader services
        broadcast = LEADER_DOMAIN_BROADCAST_SERVICES.get(domain.lower(), frozenset())
        if service.lower() in broadcast:
            context = context or Context()
            _, role = await self._async_caller(context)
            return await self._broadcast_standby(args, role=role)

        # 4. On Standby: forward whole unsplit call to the leader
        leader = self.leases.leader()
        if leader is None or leader == self.node_id:
            raise HomeAssistantError(
                f"Cannot run {domain}.{service}: the cluster has no leader"
            )

        context = context or Context()
        merged = {**(service_data or {}), **(target or {})}
        selection = TargetSelection(merged)
        targets: set[str] = set()
        if selection.has_any_target and ENTITY_MATCH_ALL not in selection.entity_ids:
            selected = async_extract_referenced_entity_ids(self.hass, selection)
            targets = selected.referenced | selected.indirectly_referenced

        role = await self._async_authorize(context, targets)
        return await self._remote(
            leader,
            domain,
            service,
            service_data,
            blocking,
            context,
            target,
            return_response,
            role,
        )

    async def _async_run_as_leader(self, args: _CallArgs) -> ServiceResponse:
        """Run a call here as the leader's work.

        Tracked like a forwarded call: while it runs, the lease is not
        released cleanly (the watchdog stays armed), and when leadership
        ends it gets the unload grace to finish, then it is cancelled.
        A blocking call runs in a task of its own for that; its caller
        gets an error if it is cancelled so, or is cancelled with it.
        """
        domain, service, *_ = args
        blocking = args[3]
        if not blocking:
            return await self._local(*args, self._leader_tasks)
        task = self.hass.async_create_task(
            self._local(*args),
            f"cluster leader call {domain}.{service}",
            eager_start=True,
        )
        if task.done():
            return task.result()
        self._leader_tasks.add(task)
        task.add_done_callback(self._leader_tasks.discard)
        try:
            return await task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if task.cancelled() and (current is None or not current.cancelling()):
                raise HomeAssistantError(
                    f"{domain}.{service} was stopped: node {self.node_id} stopped"
                    " leading the cluster"
                ) from None
            raise

    def _broadcast(
        self, args: _CallArgs, *, role: str, remote_errors_raise: bool = True
    ) -> None:
        domain, service, service_data, blocking, context, target, return_response = args
        for node in sorted(self._other_nodes()):
            coro = self._remote(
                node,
                domain,
                service,
                service_data,
                blocking,
                context,
                target,
                return_response,
                role,
            )
            self._spawn(coro if remote_errors_raise else _log_errors(coro))

    async def _broadcast_standby(
        self, args: _CallArgs, *, role: str
    ) -> ServiceResponse:
        domain, service, service_data, blocking, context, target, return_response = args
        local_result = await self._local(*args)
        leader = self.leases.leader()
        if leader is not None and leader != self.node_id:
            coro = self._remote(
                leader,
                domain,
                service,
                service_data,
                blocking,
                context,
                target,
                return_response,
                role,
            )
            if blocking:
                with suppress(Exception):
                    await coro
            else:
                self._spawn(_log_errors(coro))
        return local_result

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> None:
        task = self.hass.async_create_background_task(
            _log_errors(coro), f"cluster {self.node_id} service call"
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _remote(
        self,
        node: str,
        domain: str,
        service: str,
        service_data: dict[str, Any] | None,
        blocking: bool,
        context: Context | None,
        target: dict[str, Any] | None,
        return_response: bool,
        role: str,
    ) -> ServiceResponse:
        nc = self.coordinator.nc
        if nc is None or not self.coordinator.connected:
            raise RemoteServiceError(
                f"Cannot reach node {node} for {domain}.{service}: NATS is down"
            )
        ctx = context or Context()
        if ctx.user_id is None and role == ROLE_ADMIN:
            role = ROLE_SYSTEM
        request = json_bytes(
            {
                "origin": self.node_id,
                "instance": self.leases.incarnation,
                "domain": domain,
                "service": service,
                "service_data": service_data or {},
                "target": target,
                "blocking": blocking,
                "return_response": return_response,
                "role": role,
                # Users are per node: the role is what the receiver uses.
                "context": {"id": ctx.id, "parent_id": ctx.parent_id},
            }
        )
        try:
            msg = await nc.request(
                RPC_SUBJECT.format(to=node, origin=self.node_id),
                request,
                timeout=self.rpc_timeout,
            )
        except Exception as err:
            raise RemoteServiceError(
                f"Node {node} did not answer {domain}.{service}: {type(err).__name__}"
            ) from err
        reply = json_loads(msg.data)
        if not isinstance(reply, dict):
            raise RemoteServiceError(f"Node {node} sent a malformed reply")
        if reply.get("ok"):
            return reply.get("response")  # type: ignore[return-value]
        err_payload = reply.get("error")
        if not isinstance(err_payload, dict):
            err_payload = {}
        raise _rebuild_error(err_payload, domain, service)

    async def _async_on_rpc(self, msg: Msg) -> None:
        if self._inbound >= MAX_INBOUND_RPC:
            with suppress(Exception):
                await msg.respond(
                    json_bytes(
                        {
                            "ok": False,
                            "error": {
                                "type": "HomeAssistantError",
                                "message": f"Node {self.node_id} is busy",
                            },
                        }
                    )
                )
            return
        self._inbound += 1
        task = self.hass.async_create_background_task(
            self._async_handle_rpc(msg), f"cluster {self.node_id} rpc"
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        self._rpc_tasks.add(task)
        task.add_done_callback(self._rpc_tasks.discard)
        task.add_done_callback(self._async_rpc_done)

    def _async_rpc_done(self, _: asyncio.Task[None]) -> None:
        self._inbound -= 1

    async def _async_remote_user_id(self, role: str) -> str:
        """Id of the system user that runs calls of remote users."""
        if role == ROLE_ADMIN:
            group_id = GROUP_ID_ADMIN
        elif role == ROLE_USER:
            group_id = GROUP_ID_USER
        else:
            group_id = GROUP_ID_READ_ONLY
        name = f"{REMOTE_USER_NAME} ({role})"
        async with self._remote_user_lock:
            if (user_id := self._remote_user_ids.get(role)) is None:
                for user in await self.hass.auth.async_get_users():
                    if user.system_generated and user.name == name:
                        break
                else:
                    user = await self.hass.auth.async_create_system_user(
                        name, group_ids=[group_id]
                    )
                user_id = self._remote_user_ids[role] = user.id
            return user_id

    async def _async_handle_rpc(self, msg: Msg) -> None:
        try:
            req = _parse_request(msg.data)
            # NATS permissions let each node publish only subjects that end
            # with its own id: that is the sender, not what the body says.
            origin = _subject_origin(msg.subject)
            _check_peer_identity(
                origin,
                req.get("origin"),
                req.get("instance"),
                self.node_id,
                self.leases.peer_incarnation(origin or ""),
            )

            _check_rpc_authority(
                req["domain"],
                req["service"],
                self.leases.is_leader,
            )

            ctx = req.get("context") or {}
            # Users exist per node, so a user id from another node means
            # nothing here. The call runs as this node's system user of the
            # caller's role (admin, user or read-only); Core checks the
            # permissions of that role against this node's registries.
            role = req.get("role")
            user_id: str | None
            if role == ROLE_SYSTEM:
                user_id = None  # an internal call, as on a single instance
            else:
                if role not in _ROLE_RANK:
                    role = ROLE_READ_ONLY
                user_id = await self._async_remote_user_id(role)

            context = Context(
                user_id=user_id, parent_id=ctx.get("parent_id"), id=ctx.get("id")
            )

            # Leadership may have ended while the user was looked up.
            _check_rpc_authority(
                req["domain"],
                req["service"],
                self.leases.is_leader,
            )

            response = await self._local(
                req["domain"],
                req["service"],
                req.get("service_data"),
                bool(req.get("blocking")),
                context,
                req.get("target"),
                bool(req.get("return_response")),
                # Answered when the service is started, as the caller asked:
                # the lease is still not released while it runs.
                self._service_tasks,
            )
            reply = {"ok": True, "response": response}
        except Exception as err:  # noqa: BLE001
            reply = {"ok": False, "error": _describe_error(err)}
        try:
            await msg.respond(json_bytes(reply))
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Could not answer RPC: %s", err)


def _user_role(user: User) -> str:
    if user.is_admin:
        return ROLE_ADMIN
    if any(group.id == GROUP_ID_USER for group in user.groups):
        return ROLE_USER
    return ROLE_READ_ONLY


def _check_rpc_authority(domain: str, service: str, is_leader: bool) -> None:
    broadcast = LEADER_DOMAIN_BROADCAST_SERVICES.get(domain.lower(), frozenset())
    is_broadcast = service.lower() in broadcast
    if not is_leader and not is_broadcast:
        raise HomeAssistantError(
            f"Cannot run {domain}.{service}: this node is not the cluster leader"
        )


def _subject_origin(subject: str) -> str | None:
    """The sender of an RPC call: ``hass.rpc.<to>.from.<origin>``."""
    parts = subject.split(".")
    if len(parts) != 5 or parts[:2] != ["hass", "rpc"] or parts[3] != "from":
        return None
    return parts[4]


def _check_peer_identity(
    origin: str | None,
    claimed: str | None,
    instance: str | None,
    local_node_id: str,
    peer_incarnation: str | None,
) -> None:
    if not origin or origin == local_node_id or not NODE_ID_RE.match(origin):
        raise HomeAssistantError(f"RPC call from unverified peer {origin}")
    # The sender is the one the subject names. The body must agree, and must
    # name the run of that node that is a member now: a call without that is
    # not from this protocol.
    if claimed != origin:
        raise HomeAssistantError(
            f"RPC call from {origin} claims to come from {claimed}"
        )
    if not instance or not peer_incarnation or instance != peer_incarnation:
        raise HomeAssistantError(f"RPC call from unverified incarnation for {origin}")


def _parse_request(data: bytes) -> dict[str, Any]:
    req = json_loads(data)
    if not isinstance(req, dict):
        raise HomeAssistantError("Malformed cluster RPC request")
    return req


async def _log_errors(coro: Coroutine[Any, Any, Any]) -> None:
    try:
        await coro
    except Exception as err:  # noqa: BLE001
        _LOGGER.error("Cluster service call failed: %s", err)


def _describe_error(err: Exception) -> dict[str, Any]:
    info: dict[str, Any] = {"type": type(err).__name__, "message": str(err)}
    if isinstance(err, HomeAssistantError):
        info.update(
            translation_domain=err.translation_domain,
            translation_key=err.translation_key,
            translation_placeholders=err.translation_placeholders,
        )
    if isinstance(err, Unauthorized):
        info["entity_id"] = err.entity_id
    return info


def _rebuild_error(info: dict[str, Any], domain: str, service: str) -> Exception:
    kind = info.get("type")
    message = info.get("message") or "Remote service call failed"
    translation = {
        k: info[k]
        for k in ("translation_domain", "translation_key", "translation_placeholders")
        if info.get(k)
    }
    if kind == "ServiceNotFound":
        return ServiceNotFound(domain, service)
    if kind == "UnknownUser":
        return UnknownUser()
    if kind == "Unauthorized":
        return Unauthorized(entity_id=info.get("entity_id"), permission=POLICY_CONTROL)
    if kind in ("Invalid", "MultipleInvalid"):
        return probatio.Invalid(message)
    if kind == "ServiceValidationError":
        return ServiceValidationError(message, **translation)
    return HomeAssistantError(message, **translation)
