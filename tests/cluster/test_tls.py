"""Strict mode really is encrypted, and credentials stay out of the log."""

import logging
import os
from types import SimpleNamespace

import pytest

from homeassistant.cluster import _is_loopback
from homeassistant.cluster.coordinator import ClusterCoordinator
from homeassistant.exceptions import HomeAssistantError


@pytest.mark.parametrize(
    ("url", "loopback"),
    [
        ("nats://127.0.0.1:4222", True),
        ("nats://127.0.0.2:4222", True),
        ("nats://localhost:4222", True),
        ("nats://[::1]:4222", True),
        ("nats://[::ffff:127.0.0.1]:4222", True),
        ("tls://nats.example.com:4222", False),
        ("nats://10.0.0.1:4222", False),
        ("nats://localhost.example.com:4222", False),
        ("nats://127.0.0.1.example.com:4222", False),
    ],
)
def test_loopback_is_decided_by_address(url: str, loopback: bool) -> None:
    """Only this host's own addresses may go without encryption."""
    assert _is_loopback(url) is loopback


@pytest.mark.usefixtures("socket_enabled")
async def test_strict_mode_refuses_a_server_without_tls(nats_url: str) -> None:
    """A ``tls://`` URL alone connects in plaintext to a server without TLS.

    nats-py upgrades the connection only if the server offers TLS, so the
    coordinator has to look at the connection itself.
    """
    coordinator = ClusterCoordinator(
        nats_url.replace("nats://", "tls://"),
        "node_a",
        lease_ttl=1.5,
        require_tls=True,
    )

    with pytest.raises(HomeAssistantError, match="not encrypted"):
        await coordinator.async_connect()

    assert coordinator.nc is None
    assert not coordinator.connected


@pytest.mark.usefixtures("socket_enabled")
async def test_strict_mode_refuses_a_user_that_may_publish_as_any_node(
    nats_url: str,
) -> None:
    """The sender of a call is its subject: only the server's permissions bind it."""
    coordinator = ClusterCoordinator(
        nats_url, "node_a", lease_ttl=1.5, require_acl=True
    )

    with pytest.raises(HomeAssistantError, match="may publish as any node"):
        await coordinator.async_connect()

    assert coordinator.nc is None


@pytest.mark.usefixtures("socket_enabled")
async def test_permissive_mode_accepts_a_server_without_tls(nats_url: str) -> None:
    """Without the requirement the connection is as before."""
    coordinator = ClusterCoordinator(nats_url, "node_a", lease_ttl=1.5)
    await coordinator.async_connect()
    try:
        assert coordinator.connected
        assert coordinator._is_encrypted() is False
    finally:
        await coordinator.async_close()


def test_encryption_is_read_from_the_connection() -> None:
    """A connection whose transport cannot be examined is not trusted."""
    coordinator = ClusterCoordinator(
        "nats://x", "node_a", lease_ttl=1.5, require_tls=True
    )
    coordinator.nc = SimpleNamespace(  # type: ignore[assignment]
        _transport=SimpleNamespace(
            _io_writer=SimpleNamespace(get_extra_info=lambda name: object())
        )
    )
    assert coordinator._is_encrypted() is True
    assert coordinator._connection_allowed()

    coordinator.nc = SimpleNamespace(_transport=object())  # type: ignore[assignment]
    assert coordinator._is_encrypted() is None
    assert not coordinator._connection_allowed()


@pytest.mark.usefixtures("socket_enabled")
async def test_credentials_in_the_url_are_not_logged(
    nats_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    """The log names the host and port of the server, never its URL."""
    caplog.set_level(logging.INFO, logger="homeassistant.cluster")
    host = nats_url.removeprefix("nats://")
    coordinator = ClusterCoordinator(
        f"nats://someone:hunter2@{host}", "node_a", lease_ttl=1.5
    )
    await coordinator.async_connect()
    await coordinator.async_close()

    assert "connected to NATS" in caplog.text
    assert "hunter2" not in caplog.text
    assert "someone" not in caplog.text


@pytest.mark.skipif(
    not os.environ.get("CLUSTER_TEST_NATS_TLS_URL"),
    reason="needs a NATS server with TLS (CLUSTER_TEST_NATS_TLS_URL, _TLS_CA)",
)
@pytest.mark.usefixtures("socket_enabled")
async def test_strict_mode_accepts_a_server_with_tls() -> None:
    """With TLS, and the authority that signed the certificate, strict connects."""
    coordinator = ClusterCoordinator(
        os.environ["CLUSTER_TEST_NATS_TLS_URL"],
        "node_a",
        lease_ttl=1.5,
        require_tls=True,
        ca_file=os.environ["CLUSTER_TEST_NATS_TLS_CA"],
    )
    await coordinator.async_connect()
    try:
        assert coordinator.connected
        assert coordinator._is_encrypted() is True
    finally:
        await coordinator.async_close()
