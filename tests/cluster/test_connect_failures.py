"""A first connection that cannot work fails with the reason, and is reported.

nats-py retries a failed first connection for ever and only logs the error:
without the fix these tests run into their timeout. A fake server answers the
handshake the way a real one does when it rejects the client.
"""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
import datetime as dt
import json
import ssl
from unittest.mock import AsyncMock, patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
import pytest

from homeassistant.cluster import CONFIG_SCHEMA, ClusterNode
from homeassistant.cluster.const import DOMAIN
from homeassistant.cluster.coordinator import ClusterCoordinator, ConnectionRefused
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import issue_registry as ir
from homeassistant.util import dt as dt_util

from .conftest import NodeFactory, cluster_config
from .test_init import wait_for

REFUSED = "cluster_connection_refused"


def _info(**extra: object) -> bytes:
    info = {
        "server_id": "fake",
        "server_name": "fake",
        "version": "2.15.3",
        "go": "go1.25",
        "host": "127.0.0.1",
        "port": 0,
        "max_payload": 1048576,
        "proto": 1,
        "headers": True,
        "jetstream": True,
        **extra,
    }
    return b"INFO " + json.dumps(info).encode() + b"\r\n"


async def _rejects_credentials(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    writer.write(_info(auth_required=True))
    await writer.drain()
    await reader.readline()  # CONNECT
    writer.write(b"-ERR 'Authorization Violation'\r\n")
    await writer.drain()
    writer.close()


def _self_signed(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str]:
    """Return a certificate and key file of a certificate nobody trusts."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = dt_util.utcnow()
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    folder = tmp_path_factory.mktemp("untrusted")
    cert_file, key_file = str(folder / "cert.pem"), str(folder / "key.pem")
    with open(cert_file, "wb") as out:
        out.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(key_file, "wb") as out:
        out.write(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
    return cert_file, key_file


@pytest.fixture
async def fake_server(
    socket_enabled: None,
) -> AsyncGenerator[Callable[[Callable[..., Awaitable[None]]], Awaitable[str]]]:
    """Start a server with a handler; return its URL."""
    servers: list[asyncio.Server] = []

    async def _start(handler: Callable[..., Awaitable[None]]) -> str:
        server = await asyncio.start_server(handler, "127.0.0.1", 0)
        servers.append(server)
        return f"nats://127.0.0.1:{server.sockets[0].getsockname()[1]}"

    yield _start
    for server in servers:
        server.close()
        await server.wait_closed()


async def test_rejected_credentials_fail_the_first_connection(
    fake_server: Callable[[Callable[..., Awaitable[None]]], Awaitable[str]],
) -> None:
    """A wrong password names its cause instead of hanging until the timeout."""
    url = await fake_server(_rejects_credentials)
    coordinator = ClusterCoordinator(
        url, "node_a", lease_ttl=1.5, user="node_a", password="wrong"
    )

    async with asyncio.timeout(5):
        with pytest.raises(HomeAssistantError, match="Authorization Violation"):
            await coordinator.async_connect()

    assert not coordinator.connected
    await coordinator.async_close()


async def test_a_rejected_password_is_not_logged_or_reported(
    hass: HomeAssistant,
    issue_registry: ir.IssueRegistry,
    fake_server: Callable[[Callable[..., Awaitable[None]]], Awaitable[str]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The repair and the log name the cause, never the user or the password."""
    url = await fake_server(_rejects_credentials)
    host = url.removeprefix("nats://")
    node = ClusterNode(
        hass,
        CONFIG_SCHEMA(
            cluster_config(
                "node_a",
                f"nats://someone:hunter2@{host}",
                nats_user="node_b",
                nats_password="s3cret_other",
            )
        )[DOMAIN],
    )
    try:
        with pytest.raises(ConnectionRefused):
            await node.async_start(5)
        issue = issue_registry.async_get_issue("homeassistant", REFUSED)
        assert issue is not None
        reason = issue.translation_placeholders["reason"]
        assert "Authorization Violation" in reason
    finally:
        await node.async_close()

    for text in (caplog.text, reason):
        for secret in ("hunter2", "someone", "s3cret_other"):
            assert secret not in text


async def test_one_bad_server_among_good_ones_does_not_end_the_attempt(
    fake_server: Callable[[Callable[..., Awaitable[None]]], Awaitable[str]],
    nats_url: str,
) -> None:
    """A pool with a server that rejects and one that works connects."""
    bad = await fake_server(_rejects_credentials)
    for _ in range(5):  # the pool is shuffled: any order must work
        coordinator = ClusterCoordinator(
            f"{bad},{nats_url}", "node_a", lease_ttl=1.5, connect_timeout=2
        )
        await coordinator.async_connect()
        try:
            assert coordinator.connected
        finally:
            await coordinator.async_close()


async def test_untrusted_certificate_fails_the_first_connection(
    fake_server: Callable[[Callable[..., Awaitable[None]]], Awaitable[str]],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    """A server certificate the client does not trust names the TLS handshake."""
    cert_file, key_file = _self_signed(tmp_path_factory)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_file, key_file)

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(_info(tls_required=True))
        await writer.drain()
        try:
            await writer.start_tls(context)
            await reader.read(1)
        except ssl.SSLError, ConnectionError:
            pass  # the client refused the certificate: expected
        finally:
            writer.close()

    url = await fake_server(serve)
    coordinator = ClusterCoordinator(
        url.replace("nats://", "tls://"), "node_a", lease_ttl=1.5, require_tls=True
    )

    async with asyncio.timeout(5):
        with pytest.raises(HomeAssistantError, match="TLS handshake failed"):
            await coordinator.async_connect()

    assert not coordinator.connected
    await coordinator.async_close()


async def test_a_server_that_is_down_is_still_retried(
    fake_server: Callable[[Callable[..., Awaitable[None]]], Awaitable[str]],
) -> None:
    """Only errors that waiting cannot fix end the attempt: the caller's timeout does the rest."""
    refused = await fake_server(_rejects_credentials)
    port = int(refused.rsplit(":", 1)[1])
    coordinator = ClusterCoordinator(
        f"nats://127.0.0.1:{port + 1}", "node_a", lease_ttl=1.5, connect_timeout=0.5
    )

    with pytest.raises(TimeoutError):
        async with asyncio.timeout(1.5):
            await coordinator.async_connect()

    assert not coordinator.connected
    await coordinator.async_close()


async def test_a_refused_connection_is_a_repair_until_one_is_accepted(
    issue_registry: ir.IssueRegistry, make_node: NodeFactory
) -> None:
    """Strict mode refusing the server shows its reason; the repair goes with it."""
    node = await make_node(
        "node_a",
        nats_user="node_a",
        nats_password="secret",
        security="strict",
    )
    cluster = node.cluster

    issue = issue_registry.async_get_issue("homeassistant", REFUSED)
    assert issue is not None
    assert "may publish as any node" in issue.translation_placeholders["reason"]
    assert not cluster.started

    # The server side is fixed (here: the check passes); the retry connects.
    with patch.object(ClusterCoordinator, "_async_check_acl", AsyncMock()):
        await wait_for(lambda: cluster.started, timeout=10)

    assert issue_registry.async_get_issue("homeassistant", REFUSED) is None
