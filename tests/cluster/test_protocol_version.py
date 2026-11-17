"""The protocol version of what nodes write, and the repair issue of a stuck node.

Member keys, leases and entry records carry it; a node that cannot take over says
why. Tests against a real NATS JetStream server.
"""

import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager, suppress
import json
import time
from typing import Any
from unittest.mock import patch

import pytest

from homeassistant.cluster.const import (
    ENTRY_KEY_PREFIX,
    LEADER_KEY,
    MEMBER_KEY_PREFIX,
    PROTOCOL_VERSION,
)
from homeassistant.cluster.lease_manager import (
    PROMOTION_FAILURES_BEFORE_ISSUE,
    LeaseManager,
)
from homeassistant.cluster.protocol import (
    UnsupportedProtocol,
    check_record,
    is_supported,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import __version__ as HA_VERSION
from homeassistant.helpers import issue_registry as ir

from .conftest import (
    DEV_ENTRY_ID,
    Node,
    NodeFactory,
    open_entry_payload,
    seal_entry_payload,
)
from .test_init import wait_for

LOADED = ConfigEntryState.LOADED
ENTRY_KEY = f"{ENTRY_KEY_PREFIX}{DEV_ENTRY_ID}"
CANNOT_TAKE_OVER = "cluster_cannot_take_over"


def _issue(node: Node, issue_id: str = CANNOT_TAKE_OVER) -> ir.IssueEntry | None:
    return node.issue_registry.async_get_issue("homeassistant", issue_id)


@asynccontextmanager
async def foreign_member(
    node: Node, name: str, protocol: Any, ha_version: str | None = None
) -> AsyncGenerator[Callable[[], None]]:
    """A node of another version that renews its member key; yields ``stop``.

    ``protocol=None`` leaves the field out, like a node from before it existed.
    Stopping only stops the renewals: the key then expires on the server, which
    sends no delete (that is the case under test).
    """
    kv = node.cluster.coordinator.kv_leases
    running = True

    async def renew() -> None:
        since = None
        while running:
            payload: dict[str, Any] = {
                "node": name,
                "incarnation": "foreign-incarnation",
                "since": since,
                "grace": 0.8,
            }
            if protocol is not None:
                payload["protocol"] = protocol
            if ha_version is not None:
                payload["ha_version"] = ha_version
            with suppress(Exception):
                since = since or await kv.put(
                    f"{MEMBER_KEY_PREFIX}{name}", json.dumps(payload).encode()
                )
                await kv.put(f"{MEMBER_KEY_PREFIX}{name}", json.dumps(payload).encode())
            await asyncio.sleep(0.3)

    task = asyncio.create_task(renew())

    def stop() -> None:
        nonlocal running
        running = False

    try:
        yield stop
    finally:
        running = False
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


def test_only_the_integer_of_this_version_is_supported() -> None:
    """A missing field is version 1; ``True``, ``1.0`` and text are not a version."""
    assert PROTOCOL_VERSION == 1
    assert is_supported({})
    assert is_supported({"protocol": 1})
    assert is_supported("not an object")  # the content checks come later
    for other in (2, 0, True, 1.0, "1", None, [1]):
        assert not is_supported({"protocol": other}), other
    with pytest.raises(UnsupportedProtocol, match="entry.x .*version 2"):
        check_record("entry.x", {"protocol": 2})


async def test_everything_a_node_writes_carries_the_protocol_version(
    make_node: NodeFactory,
) -> None:
    """Member key, lease, entry record, removal announcement, tombstone, inventory."""
    a = await make_node("node_a")
    await wait_for(lambda: a.entry.state is LOADED)
    leases = a.cluster.coordinator.kv_leases
    storage = a.cluster.coordinator.kv_storage

    for key in (LEADER_KEY, f"{MEMBER_KEY_PREFIX}node_a"):
        assert json.loads((await leases.get(key)).value)["protocol"] == 1, key
    await wait_for(lambda: a.cluster.storage._last_hash.get(ENTRY_KEY) is not None)
    record = json.loads((await storage.get(ENTRY_KEY)).value)
    assert record["protocol"] == 1
    assert record["origin"] == "node_a"
    assert json.loads((await storage.get("inventory")).value)["protocol"] == 1

    written: list[tuple[str, bytes]] = []
    original = a.cluster.coordinator.async_fenced_put

    async def recording(key: str, value: bytes, *args: Any, **kwargs: Any) -> Any:
        written.append((key, value))
        return await original(key, value, *args, **kwargs)

    with patch.object(a.cluster.coordinator, "async_fenced_put", recording):
        await a.hass.config_entries.async_remove(DEV_ENTRY_ID)
        await wait_for(
            lambda: any(b'"removed"' in v for k, v in written if k == ENTRY_KEY)
        )
    kinds = set()
    for key, value in written:
        if key != ENTRY_KEY:
            continue
        payload = open_entry_payload(key, value)
        assert payload["protocol"] == 1, payload
        kinds.add("removing" if payload.get("removing") else "removed")
    assert kinds == {"removing", "removed"}


async def test_member_of_another_version_blocks_the_takeover(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """No node takes over while a member it cannot talk to is alive.

    Before the version check b took over the moment a released the lease.
    """
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)

    async with foreign_member(a, "node_z", 2) as stop:
        await wait_for(lambda: b.cluster.leases.incompatible_peers == {"node_z": 2})
        # A leader that sees such a member keeps leading.
        await asyncio.sleep(2.0)
        assert a.cluster.leases.is_leader
        assert a.entry.state is LOADED
        assert not _issue(a)

        await a.cluster.leases.async_stop(release=True)
        await wait_for(lambda: _issue(b) is not None, timeout=10)
        issue = _issue(b)
        assert issue is not None
        assert issue.translation_placeholders is not None
        assert issue.translation_placeholders["node"] == "node_b"
        assert "node_z" in issue.translation_placeholders["reason"]
        assert "version 2" in issue.translation_placeholders["reason"] or (
            "2" in issue.translation_placeholders["reason"]
        )
        await asyncio.sleep(3.0)  # longer than the lease and the grace
        assert not b.cluster.leases.is_leader
        assert b.entry.state is not LOADED
        assert "does not take over" in caplog.text

        # The member only stops renewing: its key expires without a delete.
        stop()
        await wait_for(lambda: b.entry.state is LOADED, timeout=20)

    assert b.cluster.leases.is_leader
    assert b.cluster.leases.incompatible_peers == {}
    assert _issue(b) is None


async def test_member_without_the_field_is_version_1(
    make_node: NodeFactory,
) -> None:
    """A node from before the field existed (a rolling upgrade) does not block."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)

    async with foreign_member(a, "node_old", None):
        await asyncio.sleep(1.0)
        assert b.cluster.leases.incompatible_peers == {}
        await a.cluster.leases.async_stop(release=True)
        await wait_for(lambda: b.entry.state is LOADED, timeout=15)
    assert not _issue(b)


VERSION_MISMATCH = "cluster_version_mismatch"


async def test_member_of_another_ha_version_is_a_warning_not_a_block(
    make_node: NodeFactory,
) -> None:
    """A node on another Home Assistant version raises a repair issue and no more."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)
    assert (
        json.loads(
            (
                await a.cluster.coordinator.kv_leases.get(f"{MEMBER_KEY_PREFIX}node_a")
            ).value
        )["ha_version"]
        == HA_VERSION
    )

    async with foreign_member(a, "node_z", 1, ha_version="1999.1.0") as stop:
        await wait_for(lambda: _issue(a, VERSION_MISMATCH) is not None, timeout=10)
        issue = _issue(b, VERSION_MISMATCH)
        assert issue is not None
        assert issue.translation_placeholders is not None
        versions = issue.translation_placeholders["versions"]
        assert "node_z: 1999.1.0" in versions
        assert f"node_b: {HA_VERSION}" in versions
        # It does not block: b takes over when a releases the lease.
        assert b.cluster.leases.incompatible_peers == {}
        await a.cluster.leases.async_stop(release=True)
        await wait_for(lambda: b.entry.state is LOADED, timeout=15)
        assert not _issue(b)

        # The node stops renewing: its key expires and the issue is removed.
        stop()
        await wait_for(lambda: _issue(b, VERSION_MISMATCH) is None, timeout=20)


async def test_members_of_the_same_or_unknown_ha_version_raise_nothing(
    make_node: NodeFactory,
) -> None:
    """The same version, or a node that does not say its version, is not counted."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)

    async with (
        foreign_member(a, "node_same", 1, ha_version=HA_VERSION),
        foreign_member(a, "node_old", None),
    ):
        await asyncio.sleep(2.0)
        assert not _issue(a, VERSION_MISMATCH)
        assert not _issue(b, VERSION_MISMATCH)


async def _published(node: Node) -> dict[str, Any]:
    """The leader's entry record, opened."""
    kv = node.cluster.coordinator.kv_storage
    deadline = time.monotonic() + 10
    while True:
        with suppress(Exception):
            return open_entry_payload(ENTRY_KEY, (await kv.get(ENTRY_KEY)).value)
        assert time.monotonic() < deadline, "the entry was never published"
        await asyncio.sleep(0.2)


async def test_record_of_another_version_is_not_applied_and_stops_the_takeover(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """A standby ignores it (once in the log); a promotion names it and gives up."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)
    original = await _published(a)
    kv = a.cluster.coordinator.kv_storage
    far_term = original["term"] + 1000

    foreign = {
        **original,
        "protocol": 2,
        "term": far_term,
        "entry": {**original["entry"], "data": {"token": "from-version-2"}},
    }
    for _ in range(2):
        await kv.put(ENTRY_KEY, seal_entry_payload(ENTRY_KEY, foreign))
        await asyncio.sleep(1.0)

    # Not applied, and not even the term it carries is believed.
    assert b.entry.data["token"] != "from-version-2"
    assert b.cluster.coordinator.observed_term < far_term
    assert caplog.text.count(f"Cannot read an entry record: {ENTRY_KEY}") == 1

    # b would take over, reads the record and cannot go on with it.
    with patch("homeassistant.cluster.lease_manager.HANDOFF_COOLDOWN", 0):
        await a.cluster.leases.async_stop(release=True)
        await wait_for(lambda: _issue(b) is not None, timeout=15)
        issue = _issue(b)
        assert issue is not None
        assert issue.translation_placeholders is not None
        assert ENTRY_KEY in issue.translation_placeholders["reason"]
        assert "version 2" in issue.translation_placeholders["reason"]
        assert not b.cluster.leases.is_leader
        assert b.entry.state is not LOADED
        assert b.entry.data["token"] != "from-version-2"

        # Back to a record it understands: b takes over and the issue goes.
        await kv.put(ENTRY_KEY, seal_entry_payload(ENTRY_KEY, original))
        await wait_for(lambda: b.entry.state is LOADED, timeout=30)
    assert b.cluster.leases.is_leader
    assert _issue(b) is None


async def test_record_without_the_field_is_applied(make_node: NodeFactory) -> None:
    """Records written before the field existed are version 1: applied, led with."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: a.entry.state is LOADED)
    original = await _published(a)
    kv = a.cluster.coordinator.kv_storage

    old_format = {k: v for k, v in original.items() if k != "protocol"}
    old_format["entry"] = {**original["entry"], "data": {"token": "old-format"}}
    await kv.put(ENTRY_KEY, seal_entry_payload(ENTRY_KEY, old_format))
    await wait_for(lambda: b.entry.data.get("token") == "old-format")

    await a.cluster.leases.async_stop(release=True)
    await wait_for(lambda: b.entry.state is LOADED, timeout=15)
    assert b.cluster.leases.is_leader
    assert not _issue(b)


async def test_promotion_that_keeps_failing_is_a_repair_issue_until_it_works(
    make_node: NodeFactory,
) -> None:
    """The log is not enough when no node can lead: show it after a few failures."""
    original = LeaseManager._async_run_promotion
    before_each: list[bool] = []
    broken = True

    async def run(self: LeaseManager, term: int) -> None:
        if broken:
            before_each.append(_issue(make_node.nodes[0]) is not None)
            raise RuntimeError("the promotion is broken")
        await original(self, term)

    with (
        patch("homeassistant.cluster.lease_manager.HANDOFF_COOLDOWN", 0),
        patch.object(LeaseManager, "_async_run_promotion", run),
    ):
        a = await make_node("node_a")
        await wait_for(
            lambda: _issue(a) is not None,
            timeout=60,
        )
        # Not on the first or second failure: only when it keeps happening.
        assert len(before_each) >= PROMOTION_FAILURES_BEFORE_ISSUE
        assert before_each[:PROMOTION_FAILURES_BEFORE_ISSUE] == [False] * (
            PROMOTION_FAILURES_BEFORE_ISSUE
        )
        issue = _issue(a)
        assert issue is not None
        assert issue.translation_placeholders is not None
        assert "the promotion is broken" in issue.translation_placeholders["reason"]
        assert not a.cluster.leases.is_leader

        broken = False
        await wait_for(lambda: a.entry.state is LOADED, timeout=30)
    assert a.cluster.leases.is_leader
    assert _issue(a) is None
