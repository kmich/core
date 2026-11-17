"""Regression tests for the findings of the independent review.

Tests against a real NATS JetStream server.
"""

import asyncio
import json
import logging
from pathlib import Path
import time
from typing import Any
from unittest.mock import PropertyMock, patch

import pytest

from homeassistant.cluster.const import ENTRY_KEY_PREFIX
from homeassistant.cluster.coordinator import ClusterCoordinator
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import ServiceCall, ServiceRegistry
from homeassistant.exceptions import HomeAssistantError

from .conftest import DEV_ENTRY_ID, Node, NodeFactory
from .test_init import wait_for
from .test_protocol_version import foreign_member
from .test_ui_config_sync import make_ui_node, put_record

LOADED = ConfigEntryState.LOADED
ENTRY_KEY = f"{ENTRY_KEY_PREFIX}{DEV_ENTRY_ID}"
CANNOT_TAKE_OVER = "cluster_cannot_take_over"


def _issue_reason(node: Node) -> str | None:
    issue = node.issue_registry.async_get_issue("homeassistant", CANNOT_TAKE_OVER)
    if issue is None or issue.translation_placeholders is None:
        return None
    return issue.translation_placeholders["reason"]


async def test_forwarded_non_blocking_call_keeps_the_leader_busy(
    make_node: NodeFactory,
) -> None:
    """The service of a call that was answered at its start is still work.

    The lease must not be released cleanly while it runs.
    """
    a = await make_node("node_a")
    b = await make_node("node_b")
    started = asyncio.Event()
    release = asyncio.Event()

    async def slow(call: ServiceCall) -> None:
        started.set()
        await release.wait()

    for node in (a, b):
        node.hass.services.async_register("test", "slow", slow)

    await b.hass.services.async_call("test", "slow", blocking=False)
    await asyncio.wait_for(started.wait(), 10)
    await wait_for(lambda: not a.cluster.router._rpc_tasks)  # answered already
    assert a.cluster.router.busy()

    release.set()
    await wait_for(lambda: not a.cluster.router.busy())


async def test_oversized_entry_stops_the_takeover_from_the_old_record(
    make_node: NodeFactory,
) -> None:
    """An entry that outgrew max_payload must not be taken over from its old record."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)
    await wait_for(lambda: a.cluster.storage._last_hash.get(ENTRY_KEY) is not None)
    kv = a.cluster.coordinator.kv_storage
    assert kv is not None

    with (
        patch.object(
            ClusterCoordinator,
            "max_payload",
            new_callable=PropertyMock,
            return_value=700,
        ),
        patch("homeassistant.cluster.lease_manager.HANDOFF_COOLDOWN", 0),
    ):
        a.hass.config_entries.async_update_entry(a.entry, data={"token": "x" * 4000})
        deadline = time.monotonic() + 10
        while json.loads((await kv.get(ENTRY_KEY)).value).get("oversized") is not True:
            assert time.monotonic() < deadline, "the old record was never replaced"
            await asyncio.sleep(0.2)

        await a.cluster.leases.async_stop(release=True)
        await wait_for(lambda: _issue_reason(b) is not None, timeout=15)
        assert "max_payload" in (_issue_reason(b) or "")
        assert not b.cluster.leases.is_leader
        assert b.entry.state is not LOADED
        # Nothing was applied from the marker.
        assert b.entry.data == {"token": "t1"}


async def test_unreadable_restore_state_checkpoint_is_retried(
    make_node: NodeFactory,
) -> None:
    """A read that failed is not "no checkpoint": the promotion tries again."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)
    kv = b.cluster.coordinator.kv_storage
    assert kv is not None
    original = kv.get
    broken = True

    async def get(key: str, *args: Any, **kwargs: Any) -> Any:
        if broken and key.endswith("core.restore_state"):
            raise TimeoutError
        return await original(key, *args, **kwargs)

    with (
        patch("homeassistant.cluster.lease_manager.HANDOFF_COOLDOWN", 0),
        patch.object(kv, "get", get),
    ):
        await a.cluster.leases.async_stop(release=True)
        await wait_for(lambda: _issue_reason(b) is not None, timeout=15)
        assert "restore_state" in (_issue_reason(b) or "")
        assert not b.cluster.leases.is_leader

        broken = False
        await wait_for(lambda: b.entry.state is LOADED, timeout=30)
    assert b.cluster.leases.is_leader


async def test_failed_ui_reload_is_retried_after_the_file_was_written(
    make_node: NodeFactory, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The file is current after the first attempt; the reload must still happen."""
    caplog.set_level(logging.WARNING, logger="homeassistant.cluster")
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")
    original = ServiceRegistry._async_call_local
    failing = True

    async def call_local(
        self: ServiceRegistry, domain: str, service: str, *args: Any, **kwargs: Any
    ) -> Any:
        if failing and self._hass is b.hass and service == "reload":
            raise HomeAssistantError("the reload failed")
        return await original(self, domain, service, *args, **kwargs)

    with patch.object(ServiceRegistry, "_async_call_local", call_local):
        await put_record(
            a,
            "ui.file.scripts.yaml",
            {"domain": "script", "content": "d4_marker:\n  sequence: []\n"},
        )
        await wait_for(lambda: "Could not apply" in caplog.text)
        assert b.hass.states.get("script.d4_marker") is None

        failing = False
        await wait_for(lambda: b.hass.states.get("script.d4_marker") is not None, 15)


async def test_incompatible_node_that_appears_during_the_grace_wait_blocks_leading(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """The check of the version is repeated before a node leads."""
    caplog.set_level(logging.WARNING, logger="homeassistant.cluster")
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)
    original = b.cluster.leases._async_wait_grace
    foreign = foreign_member(a, "node_z", 2)

    async def wait_grace(key: str) -> None:
        await original(key)
        # The lease was taken while no such node was known.
        await foreign.__aenter__()
        await wait_for(lambda: b.cluster.leases.incompatible_peers == {"node_z": 2})

    try:
        with patch.object(b.cluster.leases, "_async_wait_grace", wait_grace):
            await a.cluster.leases.async_stop(release=True)
            await wait_for(lambda: "gives up the lease it took" in caplog.text, 20)
            assert not b.cluster.leases.is_leader
            assert b.entry.state is not LOADED
    finally:
        await foreign.__aexit__(None, None, None)
