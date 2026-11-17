"""NATS JetStream connection and the two-tier KV buckets."""

import asyncio
from collections.abc import Callable
from contextlib import suppress
import logging
import ssl
import time
from time import monotonic
from typing import TYPE_CHECKING, Any

from nats.aio.client import Client as NATS
from nats.errors import NoRespondersError
from nats.js import JetStreamContext
from nats.js.api import DiscardPolicy, KeyValueConfig, StorageType, StreamConfig
from nats.js.errors import (
    APIError,
    BucketNotFoundError,
    KeyNotFoundError,
    KeyWrongLastSequenceError,
    NoStreamResponseError,
    NotFoundError,
)
from nats.js.kv import KV_DEL, KV_PURGE, KeyValue

from homeassistant.core import callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.json import json_bytes
from homeassistant.util.json import json_loads

from .const import (
    BUCKET_LEASES,
    BUCKET_STATES,
    BUCKET_STORAGE,
    INBOX_PREFIX,
    MIN_NATS_VERSION,
    RPC_SUBJECT,
    TERM_KEY,
)

_LOGGER = logging.getLogger(__name__)

# JetStream API timeout. Kept well below the lease TTL so that a stalled
# request can never hide a lost lease for longer than the TTL.
JS_TIMEOUT = 1.0

# JetStream error code for "stream not found".
_ERR_STREAM_NOT_FOUND = 10059

# Seconds between two bucket checks triggered by errors.
_BUCKET_CHECK_INTERVAL = 2.0

# JetStream error codes for "wrong last sequence".
_ERR_WRONG_LAST_SEQUENCE = (10071, 10164)
# A node that does not exist: a publish as it is allowed only without ACLs.
_ACL_PROBE_NODE = "acl-probe"
_ACL_PROBE_WAIT = 1.0

_HDR_EXPECTED_SEQ = "Nats-Expected-Last-Subject-Sequence"
_HDR_EXPECTED_SEQ_SUBJECT = "Nats-Expected-Last-Subject-Sequence-Subject"
_HDR_KV_OPERATION = "KV-Operation"
_HDR_ROLLUP = "Nats-Rollup"


class TermSuperseded(HomeAssistantError):
    """Another node started a newer term: this node must not write any more."""


class ConnectionRefused(HomeAssistantError):
    """This node refuses the server, or the server refuses this node.

    A certificate that is not trusted, credentials that are rejected, a server
    without TLS or a user that may publish as any node in strict mode, a server
    that is too old: waiting does not fix them, a change of the server does.
    """


class NoTerm(HomeAssistantError):
    """This node has no term to write in (yet, or the term key was lost)."""


def is_missing_stream(err: BaseException) -> bool:
    """Return True if ``err`` means that a KV bucket's stream does not exist."""
    if isinstance(
        err,
        (
            NoStreamResponseError,
            NoRespondersError,
            BucketNotFoundError,
            TimeoutError,
            asyncio.TimeoutError,
        ),
    ):
        return True
    msg = str(err).lower()
    if "timeout" in msg or "stream not found" in msg or "bucket not found" in msg:
        return True
    return (
        isinstance(err, NotFoundError)
        and getattr(err, "err_code", None) == _ERR_STREAM_NOT_FOUND
    )


async def stop_watcher(watcher: KeyValue.KeyWatcher) -> None:
    """Unsubscribe a KV watcher, never raising.

    Cancelling the task that iterates a watcher does not unsubscribe it: the
    subscription and its server-side consumer stay until the connection
    closes. Call this from a ``finally`` around the iteration.
    """
    inner_sub = getattr(getattr(watcher, "_sub", None), "_sub", None)
    cancelled: list[asyncio.Task[Any]] = []
    if (
        msg_task := getattr(inner_sub, "_wait_for_msgs_task", None)
    ) is not None and not msg_task.done():
        msg_task.cancel()
        cancelled.append(msg_task)
    jsi = getattr(inner_sub, "_jsi", None)
    for name in ("_hbtask", "_fctask"):
        if (task := getattr(jsi, name, None)) is not None and not task.done():
            task.cancel()
            cancelled.append(task)
    if cancelled:
        # Let them end: a cancelled task still runs until it sees the
        # cancellation (the heartbeat check sleeps for seconds otherwise).
        await asyncio.wait(cancelled, timeout=1)
    try:
        async with asyncio.timeout(JS_TIMEOUT):
            await watcher.stop()  # type: ignore[no-untyped-call]
    except BaseException as err:  # noqa: BLE001 - disconnected, already stopped
        _LOGGER.debug("Could not stop KV watcher: %s", err)


class ClusterCoordinator:
    """Owns the NATS connection and the KV buckets used by every other module.

    Tier 1 (memory, no flash wear): ``hass_states`` and ``hass_leases``.
    Tier 2 (file, low write rate): ``hass_storage``.

    A restart of a NATS server without persistent storage loses the buckets.
    Every module reports NATS errors with ``note_error``; an error meaning
    "stream not found", or a reconnect, asks the owner (``on_bucket_check``)
    to run ``async_ensure_buckets``.
    """

    def __init__(
        self,
        nats_url: str | list[str],
        node_id: str,
        *,
        lease_ttl: float,
        replicas: int = 1,
        user: str | None = None,
        password: str | None = None,
        token: str | None = None,
        creds: str | None = None,
        connect_timeout: float = 5.0,
        require_tls: bool = False,
        ca_file: str | None = None,
        require_acl: bool = False,
    ) -> None:
        """Initialize the coordinator.

        ``require_tls``: refuse a connection that is not encrypted. nats-py
        upgrades a connection to TLS only if the server offers it, so a
        ``tls://`` URL alone is no guarantee. ``ca_file``: the certificate
        authority that signed the server's certificate, if it is not a public
        one. ``require_acl``: refuse a connection whose NATS user may publish
        as another node. The sender of a call is taken from its subject, and
        only the server's permissions make that trustworthy.
        """
        self._require_tls = require_tls
        self._ca_file = ca_file
        self._require_acl = require_acl
        self._acl_violation = asyncio.Event()
        # Set while the first connection is made (see ``_async_first_connect``).
        self._connect_failed: asyncio.Future[str] | None = None
        self._unrecoverable = 0
        self._probing_acl = False
        self.servers = (
            [s.strip() for s in nats_url.split(",")]
            if isinstance(nats_url, str)
            else list(nats_url)
        )
        self.node_id = node_id
        self.lease_ttl = lease_ttl
        self.replicas = replicas
        self._auth: dict[str, Any] = {
            k: v
            for k, v in (
                ("user", user),
                ("password", password),
                ("token", token),
                ("user_credentials", creds),
            )
            if v
        }
        self._connect_timeout = connect_timeout
        self.nc: NATS | None = None
        self.js: JetStreamContext | None = None
        self.kv_states: KeyValue | None = None
        self.kv_leases: KeyValue | None = None
        self.kv_storage: KeyValue | None = None
        # Called in the event loop when the buckets may need recreating.
        self.on_bucket_check: Callable[[], None] | None = None
        # Called in the event loop when the server refused a write because
        # another node started a newer term.
        self.on_term_lost: Callable[[], None] | None = None
        self._last_check = 0.0
        self._stream_created: dict[str, str] = {}
        # The term this node writes in, and the revision of the term key
        # that started it (the fencing token); None while it has none.
        self.term = 0
        # The newest term seen in this node's own term or in any record.
        self.observed_term = 0
        self.fence_revision: int | None = None

    @property
    def connected(self) -> bool:
        """Return True while the NATS connection is up."""
        return self.nc is not None and self.nc.is_connected

    @property
    def max_payload(self) -> int:
        """Largest message the server accepts."""
        return self.nc.max_payload if self.nc else 1024 * 1024

    async def async_connect(self) -> None:
        """Connect to NATS and create or bind the KV buckets."""
        tls: dict[str, Any] = {}
        if self._ca_file:
            # Loading certificates reads files: not in the event loop.
            tls["tls"] = await asyncio.to_thread(
                ssl.create_default_context, cafile=self._ca_file
            )
        self.nc = await self._async_first_connect(tls)
        if not self._connection_allowed():
            await self.nc.close()
            self.nc = None
            raise ConnectionRefused(
                "The NATS connection is not encrypted, and security is strict:"
                " the server did not offer TLS. Enable TLS on the NATS server"
                " (tls { ... } in its configuration) or set security: permissive"
            )
        if self._require_acl:
            await self._async_check_acl()
        self._check_server_version()
        self.js = self.nc.jetstream(timeout=JS_TIMEOUT)

        self.kv_states = await self._bucket(self._config(BUCKET_STATES))
        self.kv_leases = await self._bucket(self._config(BUCKET_LEASES))
        self.kv_storage = await self._bucket(self._config(BUCKET_STORAGE))
        await self._async_adopt_lease_ttl()
        # Remember the streams, so that a bucket somebody else recreates is
        # noticed by the first check, not the second.
        for bucket in (BUCKET_STATES, BUCKET_LEASES, BUCKET_STORAGE):
            await self._async_remember_stream(bucket)
        _LOGGER.info(
            "Node %s connected to NATS JetStream at %s",
            self.node_id,
            # Host and port only: the URL may carry credentials.
            (
                f"{url.hostname}:{url.port}"
                if (url := self.nc.connected_url) is not None
                else "?"
            ),
        )

    async def _async_first_connect(self, tls: dict[str, Any]) -> NATS:
        """Connect for the first time, or fail with the reason.

        nats-py retries a failed first connection for ever, as it does an
        established one, and only logs the error. A certificate the client
        does not trust or credentials the server rejects never get better
        by waiting, so the attempt ends when every server of the pool has
        refused (one bad server among good ones does not end it). Anything else
        (a server that is down) is retried until the caller's timeout; the
        reconnects of an established connection are not affected.
        """
        failed: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        self._connect_failed = failed
        self._unrecoverable = 0
        nc = NATS()
        options: dict[str, Any] = {
            "servers": self.servers,
            "name": f"home-assistant-cluster-{self.node_id}",
            # Each node's user may subscribe to its own inbox only.
            "inbox_prefix": INBOX_PREFIX.format(node_id=self.node_id),
            "connect_timeout": self._connect_timeout,
            "allow_reconnect": True,
            "max_reconnect_attempts": -1,
            "reconnect_time_wait": 0.5,
            "error_cb": self._on_error,
            "disconnected_cb": self._on_disconnected,
            "reconnected_cb": self._on_reconnected,
            **tls,
            **self._auth,
        }
        connect = asyncio.ensure_future(nc.connect(**options))
        waiters: set[asyncio.Future[Any]] = {connect, failed}
        refused: str | None = None
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            if connect.done():
                connect.result()
            else:
                refused = failed.result()
        except BaseException:
            await self._async_abort_connect(nc, connect)
            raise
        finally:
            self._connect_failed = None
        if refused is not None:
            await self._async_abort_connect(nc, connect)
            raise ConnectionRefused(
                f"The NATS server did not accept this node: {refused}"
            )
        return nc

    async def _async_abort_connect(
        self, nc: NATS, connect: asyncio.Future[None]
    ) -> None:
        """Stop a first connection that failed or was cancelled."""
        connect.cancel()
        await asyncio.wait({connect}, timeout=1)
        # nats-py's own close can wait on a socket for ever: the bounded one.
        self.nc = nc
        try:
            await self.async_close()
        finally:
            self.nc = None

    async def _async_check_acl(self) -> None:
        """Refuse a NATS user that may publish as a node that does not exist.

        The server answers a publish its permissions do not allow with an
        error. If none comes, this user may call services as any node.
        """
        if TYPE_CHECKING:
            assert self.nc is not None
        self._acl_violation.clear()
        self._probing_acl = True
        try:
            await self.nc.publish(
                RPC_SUBJECT.format(to=_ACL_PROBE_NODE, origin=_ACL_PROBE_NODE), b""
            )
            await self.nc.flush()
            try:
                await asyncio.wait_for(self._acl_violation.wait(), _ACL_PROBE_WAIT)
            except TimeoutError:
                allowed = True
            else:
                allowed = False
        finally:
            self._probing_acl = False
        if allowed:
            await self.nc.close()
            self.nc = None
            raise ConnectionRefused(
                "Security is strict, but this node's NATS user may publish as any"
                " node: configure the server permissions of the README (each"
                " node publishes only hass.rpc.*.from.<its node id>), or set"
                " security: permissive"
            )

    def _is_encrypted(self) -> bool | None:
        """Return whether the connection uses TLS, or None if it cannot be told."""
        if self.nc is None:
            return None
        transport = getattr(self.nc, "_transport", None)
        writer = getattr(transport, "_io_writer", None)
        if writer is None:
            return None
        return writer.get_extra_info("ssl_object") is not None

    def _connection_allowed(self) -> bool:
        """Return False if TLS is required and the connection has none.

        A connection whose encryption cannot be determined is not allowed
        either: strict mode fails closed.
        """
        return not self._require_tls or self._is_encrypted() is True

    def _check_server_version(self) -> None:
        """Refuse a NATS server older than the one the cluster is tested on."""
        if TYPE_CHECKING:
            assert self.nc is not None
        version = self.nc.connected_server_version
        try:
            current = (version.major, version.minor)
        except ValueError:
            current = (0, 0)
        if current < MIN_NATS_VERSION:
            raise ConnectionRefused(
                f"The NATS server runs version {version}; the cluster needs"
                f" NATS {'.'.join(map(str, MIN_NATS_VERSION))} or later"
            )

    async def async_start_term(self, node_id: str) -> int:
        """Start a new leadership term; return its fencing revision.

        Increases the term key with a compare-and-set on the revision that
        was read, so two nodes can never both start the same term. Raises
        ``HomeAssistantError`` if that fails twice.
        """
        kv = self.kv_storage
        if kv is None or not self.connected:
            raise HomeAssistantError("NATS is not connected")
        self.fence_revision = None
        for _ in range(2):
            try:
                current = await kv.get(TERM_KEY)
            except KeyNotFoundError as err:
                # Deleted or purged: replace exactly that marker.
                term = 0
                last = err.entry.revision if err.entry is not None else 0
            else:
                stored = _parse(current.value).get("term")
                term = stored if isinstance(stored, int) else 0
                last = current.revision or 0
            # A recreated bucket forgets its terms; never start below one that
            # was already seen, or receivers would take the new leader's
            # records for those of a superseded one.
            term = max(term, self.observed_term)
            payload = json_bytes({"term": term + 1, "leader": node_id})
            try:
                revision = await kv.update(TERM_KEY, payload, last=last)
            except KeyWrongLastSequenceError:
                continue  # somebody else started one meanwhile: read again
            self.term = term + 1
            self.note_term(self.term)
            self.fence_revision = revision
            return revision
        raise HomeAssistantError("another node started a term at the same time")

    @staticmethod
    def is_refused_write(err: Exception) -> bool:
        """Return True if ``err`` is the server refusing a conditional write.

        The write named a sequence that is not the current one: someone wrote
        since. nats-py raises ``KeyWrongLastSequenceError`` for some such
        refusals and a plain ``APIError`` for others.
        """
        return isinstance(err, KeyWrongLastSequenceError) or (
            isinstance(err, APIError) and err.err_code in _ERR_WRONG_LAST_SEQUENCE
        )

    @callback
    def note_term(self, term: Any) -> None:
        """Remember the newest leadership term that was seen anywhere.

        Records of an older term come from a leader that was superseded: a
        receiver ignores them (see ``is_stale_term``).
        """
        if isinstance(term, int) and not isinstance(term, bool):
            self.observed_term = max(self.observed_term, term)

    def is_stale_term(self, term: Any) -> bool:
        """Return True for a record of a term older than one already seen.

        A record without a term (an older node) is never stale.
        """
        return (
            isinstance(term, int)
            and not isinstance(term, bool)
            and term < self.observed_term
        )

    @callback
    def async_end_term(self) -> None:
        """Stop writing to ``hass_storage`` (this node no longer leads)."""
        self.fence_revision = None

    async def async_fenced_put(self, key: str, value: bytes) -> int:
        """Write ``key`` in ``hass_storage`` if this node's term is current.

        Raises ``TermSuperseded`` if another node started a newer term, and
        ``NoTerm`` if this node has none (or the term key was lost with the
        bucket, until the leader starts a term again).
        """
        return await self._async_fenced(key, value, None)

    async def async_fenced_rollup(self, key: str, value: bytes) -> int:
        """Write ``key`` like ``async_fenced_put``, dropping its older values.

        For large values (add-on snapshots): the bucket keeps a history of
        every key, and a rollup keeps only this value.
        """
        return await self._async_fenced(key, value, None, rollup=True)

    async def async_fenced_purge(self, key: str) -> int:
        """Remove ``key`` and its history if this node's term is current."""
        return await self._async_fenced(key, b"", KV_PURGE, rollup=True)

    async def async_fenced_delete(self, key: str) -> int:
        """Delete ``key`` in ``hass_storage`` if this node's term is current."""
        return await self._async_fenced(key, b"", KV_DEL)

    async def _async_fenced(
        self, key: str, value: bytes, operation: str | None, *, rollup: bool = False
    ) -> int:
        if (revision := self.fence_revision) is None:
            # Nothing was sent, so nothing can note an error: ask for the
            # check that starts a term again if the term key was lost.
            self.request_bucket_check()
            raise NoTerm("this node holds no term")
        if TYPE_CHECKING:
            assert self.js is not None
        headers = {
            _HDR_EXPECTED_SEQ: str(revision),
            _HDR_EXPECTED_SEQ_SUBJECT: f"$KV.{BUCKET_STORAGE}.{TERM_KEY}",
        }
        if operation is not None:
            headers[_HDR_KV_OPERATION] = operation
        if rollup:
            headers[_HDR_ROLLUP] = "sub"
        try:
            ack = await self.js.publish(
                f"$KV.{BUCKET_STORAGE}.{key}", value, headers=headers
            )
        except APIError as err:
            if err.err_code not in _ERR_WRONG_LAST_SEQUENCE:
                raise
            await self._async_refused(revision, err)
            raise  # not reached: _async_refused always raises
        return ack.seq

    async def _async_refused(self, revision: int, refusal: APIError) -> None:
        """Find out why the server refused a write of the term ``revision``.

        The refusal names the sequence the term key had when the server
        decided. The key is read to learn what that was (another node's term,
        or a delete or purge marker), but only a read that shows that same
        sequence counts. If the key moved on meanwhile the answer is unknown
        and the write is retried: a lost term is never assumed from a read
        that may belong to a later state.
        """
        if TYPE_CHECKING:
            assert self.kv_storage is not None
        seen = _refused_sequence(refusal)
        if seen == revision:
            raise NoTerm("the write was refused for another reason; retrying")
        try:
            current = await self.kv_storage.get(TERM_KEY)
        except KeyNotFoundError as err:
            marker = err.entry.revision if err.entry is not None else 0
            if seen is not None and marker != seen:
                raise NoTerm("the term key changed after the refusal") from err
            # Lost with the bucket, not taken by another node. The bucket
            # check recreates the bucket, and the leader starts a term again.
            if self.fence_revision == revision:
                self.fence_revision = None
            self._last_check = 0.0
            self.request_bucket_check()
            raise NoTerm("the term key is missing") from err
        if seen is not None and current.revision != seen:
            raise NoTerm("the term key changed after the refusal")
        if current.revision == revision:
            raise NoTerm("the write was refused for another reason; retrying")
        self._async_term_superseded(revision, current.value)
        raise TermSuperseded("another node started a newer term")

    def _async_term_superseded(self, revision: int, value: bytes | None = None) -> None:
        if self.fence_revision != revision:
            return
        self.fence_revision = None
        data = _parse(value)
        _LOGGER.error(
            "Term %s of node %s was superseded by term %s of node %s;"
            " it stops writing and leading",
            self.term,
            self.node_id,
            data.get("term"),
            data.get("leader"),
        )
        if self.on_term_lost is not None:
            self.on_term_lost()

    def _config(self, bucket: str) -> KeyValueConfig:
        if bucket == BUCKET_LEASES:
            return KeyValueConfig(
                bucket=BUCKET_LEASES,
                history=1,
                ttl=self.lease_ttl,
                storage=StorageType.MEMORY,
                replicas=self.replicas,
            )
        if bucket == BUCKET_STATES:
            return KeyValueConfig(
                bucket=BUCKET_STATES,
                history=1,
                storage=StorageType.MEMORY,
                replicas=self.replicas,
            )
        return KeyValueConfig(
            bucket=BUCKET_STORAGE,
            history=3,
            storage=StorageType.FILE,
            replicas=self.replicas,
        )

    async def _async_adopt_lease_ttl(self) -> None:
        """Use the TTL that the lease bucket really has.

        The fence deadline must come before the server expires a lease. A
        bucket created by a node with another ``lease_ttl`` keeps its own
        TTL, so this node fences by that one instead of its configuration.
        """
        if TYPE_CHECKING:
            assert self.kv_leases is not None
        status = await self.kv_leases.status()
        if (ttl := status.ttl) and abs(ttl - self.lease_ttl) > 0.001:
            _LOGGER.warning(
                "The %s bucket expires leases after %.1fs, not after the"
                " lease_ttl of %.1fs configured on node %s, which uses %.1fs."
                " Configure the same lease_ttl on all nodes",
                BUCKET_LEASES,
                ttl,
                self.lease_ttl,
                self.node_id,
                ttl,
            )
            self.lease_ttl = ttl

    async def async_ensure_buckets(self) -> set[str]:
        """Recreate the buckets that the server lost; return their names.

        Raises if JetStream cannot answer (for example while it is still
        starting after a restart); the caller retries.
        """
        if TYPE_CHECKING:
            assert self.js is not None
        recreated: set[str] = set()
        for bucket, attr in (
            (BUCKET_LEASES, "kv_leases"),
            (BUCKET_STATES, "kv_states"),
            (BUCKET_STORAGE, "kv_storage"),
        ):
            try:
                kv = await self.js.key_value(bucket)
            except BucketNotFoundError:
                kv = await self._bucket(self._config(bucket))
                recreated.add(bucket)
            setattr(self, attr, kv)
            if await self._async_remember_stream(bucket):
                recreated.add(bucket)
        return recreated

    async def _async_remember_stream(self, bucket: str) -> bool:
        """Record when the bucket's stream was created.

        Returns True if the bucket was recreated since the last call.
        """
        if TYPE_CHECKING:
            assert self.js is not None
        try:
            info = await self.js.stream_info(f"KV_{bucket}")
        except Exception:  # noqa: BLE001
            return False
        created_str = str(info.created)
        changed = (
            bucket in self._stream_created
            and self._stream_created[bucket] != created_str
        )
        self._stream_created[bucket] = created_str
        return changed

    def note_error(self, err: BaseException) -> None:
        """Report a NATS error; a missing stream triggers a bucket check."""
        if is_missing_stream(err):
            self.request_bucket_check()

    def request_bucket_check(self) -> None:
        """Ask the owner to check the buckets, at most every few seconds."""
        if (now := monotonic()) - self._last_check < _BUCKET_CHECK_INTERVAL:
            return
        self._last_check = now
        if self.on_bucket_check is not None:
            self.on_bucket_check()

    async def _bucket(self, config: KeyValueConfig) -> KeyValue:
        """Create a bucket, or bind to it if another node created it first.

        A recreated stream starts at a sequence taken from the clock, so a
        compare-and-set condition that names a sequence of the old stream can
        never be satisfied by the new one.
        """
        if TYPE_CHECKING:
            assert self.js is not None
        try:
            await self.js.add_stream(_kv_stream(config, int(time.time() * 1000)))
            return await self.js.key_value(config.bucket)
        except Exception as err:  # noqa: BLE001 - nats raises several types here
            try:
                kv = await self.js.key_value(config.bucket)
            except BucketNotFoundError:
                raise err from None
            if await self._async_bucket_matches(config):
                # The usual case of every node but the first: the bucket
                # exists. Only its first sequence differs (see above).
                _LOGGER.debug("Bound to the existing bucket %s", config.bucket)
            else:
                _LOGGER.warning(
                    "Bound to existing bucket %s whose configuration differs from"
                    " this node's (%s); check lease_ttl/replicas match on all nodes",
                    config.bucket,
                    err,
                )
            return kv

    async def _async_bucket_matches(self, config: KeyValueConfig) -> bool:
        """Return True if the existing bucket has the settings this node wants."""
        if TYPE_CHECKING:
            assert self.js is not None
        try:
            existing = (await self.js.stream_info(f"KV_{config.bucket}")).config
        except Exception:  # noqa: BLE001 - cannot tell: the caller warns
            return False
        return (
            (existing.max_age or 0) == (config.ttl or 0)
            and existing.num_replicas == config.replicas
            and existing.max_msgs_per_subject == config.history
            and existing.storage == config.storage
        )

    async def async_close(self) -> None:
        """Drain (if connected) and close, bounded in time.

        With a dead link, nats-py's close can wait on the socket indefinitely.
        """
        if self.nc is None or self.nc.is_closed:
            return
        nc = self.nc
        subs = list(nc._subs.values())  # noqa: SLF001
        try:
            async with asyncio.timeout(3):
                if nc.is_connected:
                    with suppress(Exception):
                        await nc.drain()
                await nc.close()
        except asyncio.CancelledError:
            if (task := asyncio.current_task()) and task.cancelling():
                raise
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("NATS close on node %s: %s", self.node_id, err)
        finally:
            subs.extend(nc._subs.values())  # noqa: SLF001
            # If close timed out or link hung, force cleanup of client background tasks
            for task in (
                nc._reading_task,  # noqa: SLF001
                nc._ping_interval_task,  # noqa: SLF001
                nc._flusher_task,  # noqa: SLF001
                nc._reconnection_task,  # noqa: SLF001
            ):
                if task is not None and not task.done():
                    task.cancel()
            for sub in set(subs):
                if (
                    msg_task := getattr(sub, "_wait_for_msgs_task", None)
                ) is not None and not msg_task.done():
                    msg_task.cancel()
                if (jsi := getattr(sub, "_jsi", None)) is not None:
                    for name in ("_hbtask", "_fctask"):
                        if (
                            task := getattr(jsi, name, None)
                        ) is not None and not task.done():
                            task.cancel()
            if nc._transport is not None:  # noqa: SLF001
                nc._transport.close()  # type: ignore[no-untyped-call]  # noqa: SLF001
            await asyncio.sleep(0)

    async def _on_error(self, err: Exception) -> None:
        if isinstance(err, APIError) and err.err_code in _ERR_WRONG_LAST_SEQUENCE:
            return  # a refused conditional write; the caller handles it
        if self._probing_acl and "permissions violation" in str(err).lower():
            self._acl_violation.set()  # the answer the probe waits for
            return
        if (
            (failed := self._connect_failed) is not None
            and not failed.done()
            and _is_unrecoverable(err)
        ):
            # Every server of the pool must have refused: one bad server
            # among good ones does not end the attempt.
            self._unrecoverable += 1
            if self._unrecoverable >= len(self.servers):
                failed.set_result(_describe(err))
        _LOGGER.warning("NATS error on node %s: %s", self.node_id, err)

    async def _on_disconnected(self) -> None:
        _LOGGER.warning("Node %s lost its NATS connection", self.node_id)

    async def _on_reconnected(self) -> None:
        if not self._connection_allowed():
            # A different server of the pool, without TLS: never talk to it.
            _LOGGER.error(
                "Node %s reconnected to a NATS server without TLS while security"
                " is strict; closing the connection",
                self.node_id,
            )
            if self.nc is not None:
                await self.nc.close()
            return
        _LOGGER.info("Node %s reconnected to NATS", self.node_id)
        # A restarted server may have lost the memory-backed buckets.
        self._last_check = 0.0
        self.request_bucket_check()


def _parse(value: bytes | None) -> dict[str, Any]:
    try:
        data = json_loads(value or b"{}")
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _is_unrecoverable(err: Exception) -> bool:
    """Return True if retrying a first connection cannot fix ``err``."""
    if isinstance(err, ssl.SSLError):
        return True
    text = str(err).lower()
    return "authorization" in text or "authentication" in text


def _describe(err: Exception) -> str:
    if isinstance(err, ssl.SSLError):
        return (
            f"the TLS handshake failed ({err}); check the certificate of the"
            " server and ca_file"
        )
    return f"{err} (check nats_user, nats_password, nats_token or nats_creds)"


def _refused_sequence(refusal: APIError) -> int | None:
    """Return the sequence a "wrong last sequence" refusal reports."""
    _, _, tail = (refusal.description or "").rpartition(":")
    try:
        return int(tail)
    except ValueError:
        return None


def _kv_stream(config: KeyValueConfig, first_seq: int) -> StreamConfig:
    """Return the stream of a key-value bucket, starting at ``first_seq``.

    The same settings as ``JetStreamContext.create_key_value``, which cannot
    set the first sequence.
    """
    return StreamConfig(
        name=f"KV_{config.bucket}",
        description=config.description,
        subjects=[f"$KV.{config.bucket}.>"],
        allow_direct=config.direct,
        allow_rollup_hdrs=True,
        allow_msg_ttl=True,
        deny_delete=True,
        discard=DiscardPolicy.NEW,
        duplicate_window=min(120.0, config.ttl) if config.ttl else 120.0,
        max_age=config.ttl,
        max_bytes=config.max_bytes,
        max_consumers=-1,
        max_msg_size=config.max_value_size,
        max_msgs=-1,
        max_msgs_per_subject=config.history,
        num_replicas=config.replicas,
        storage=config.storage,
        republish=config.republish,
        first_seq=first_seq,
    )
