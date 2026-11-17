"""End-to-end: two Home Assistant processes, one NATS, a real network partition."""

from __future__ import annotations

import asyncio
import json
import time

import pytest

from .conftest import (
    NETWORK,
    SUN_ENTITY,
    SUN_ENTRY,
    Cluster,
    container,
    is_running,
    logs,
    other,
    run,
)

pytestmark = pytest.mark.e2e

TTL = 3.0
FENCE_LOG = "releasing it locally to avoid split brain"


async def test_entry_runs_on_the_leader_only(cluster: Cluster) -> None:
    """One leader runs the entry and publishes; a standby refuses to act."""
    leader = await cluster.lease_owner("_leader")
    assert leader is not None
    standby = other(leader)

    # A standby refuses a call that is not a broadcast, by design.
    refused = await cluster.rpc(
        standby,
        "input_boolean",
        "turn_on",
        {"entity_id": "input_boolean.cluster_probe"},
    )
    assert refused["ok"] is False, refused
    # The leader runs it.
    accepted = await cluster.rpc(
        leader, "input_boolean", "toggle", {"entity_id": "input_boolean.cluster_probe"}
    )
    assert accepted["ok"], accepted

    # Every published state update must come from the leader, and the stream
    # must go quiet: no node echoes another node's write back.
    kv = await cluster.js.key_value("hass_states")
    watcher = await kv.watchall()
    seen: list[tuple[str, str]] = []
    end = time.monotonic() + 8
    while time.monotonic() < end:
        try:
            update = await watcher.updates(timeout=1)
        except Exception:  # noqa: BLE001
            continue
        if update is None or update.operation:
            continue
        seen.append((update.key, json.loads(update.value)["origin"]))
    await watcher.stop()
    assert seen, "no state was published"
    for key, origin in seen:
        assert origin == leader, (key, origin)

    sun = await cluster.state(SUN_ENTITY)
    assert sun is not None
    assert sun["origin"] == leader
    assert sun["config_entry_id"] == SUN_ENTRY
    assert await cluster.lease_owner("_leader") == leader


async def test_network_drop_promotes_standby(cluster: Cluster) -> None:
    """The standby takes over after a network drop; the old leader fences."""
    old = await cluster.lease_owner("_leader")
    assert old is not None
    new = other(old)

    cut = time.monotonic()
    await run("docker", "network", "disconnect", NETWORK, container(old))

    async def promoted() -> bool:
        return await cluster.lease_owner("_leader") == new

    await cluster.wait(promoted, timeout=20)
    lease_moved = time.monotonic() - cut

    async def serving() -> bool:
        state = await cluster.state(SUN_ENTITY)
        return state is not None and state["origin"] == new

    await cluster.wait(serving, timeout=30)
    failover = time.monotonic() - cut
    print(  # noqa: T201
        f"lease moved after {lease_moved:.1f}s, standby serving after {failover:.1f}s"
    )
    assert lease_moved < TTL + 4

    # The partitioned node fenced itself (it cannot see the bucket).
    assert FENCE_LOG in await logs(old)
    assert "Entity id already exists" not in await logs(new)

    # Heal the partition: the old leader stays a standby, no flapping, and it
    # is a member again. The watchdog must not have ended it.
    await run("docker", "network", "connect", NETWORK, container(old))

    async def member_again() -> bool:
        return await cluster.lease_owner(f"_node.{old}") == old

    await cluster.wait(member_again, timeout=30)
    for _ in range(10):
        await asyncio.sleep(1)
        assert await cluster.lease_owner("_leader") == new
    sun = await cluster.state(SUN_ENTITY)
    assert sun is not None
    assert sun["origin"] == new
    assert await is_running(old), "the old leader's container stopped"
