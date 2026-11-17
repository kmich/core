"""Fixtures for the docker-compose harness (two real HA processes + NATS).

Run from the harness directory, with pytest, pytest-asyncio and nats-py
installed (see the README, "Multi-container end-to-end harness")::

    pytest -c pytest.ini --rootdir . --confcutdir . -m e2e

The cluster is observed and driven only through NATS: lease and state
buckets for observation, the cluster's own RPC subject to act on the leader.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import suppress
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any

import nats
import pytest

HARNESS = Path(__file__).resolve().parent.parent
RUNTIME = HARNESS / "runtime"
TEMPLATE = HARNESS / "template"
COMPOSE_FILE = HARNESS / "docker-compose.yml"
NATS_URL = "nats://127.0.0.1:14222"
SUN_ENTRY = "01JHARNESSSUNENTRY00000000"
# sun.sun has no config entry; this sensor of the sun entry is replicated.
SUN_ENTITY = "sensor.sun_next_dawn"
NETWORK = "hass-cluster_cluster"
NODES = ("node_a", "node_b")
# The test client calls services like a node does: a call is accepted only
# from a member, and names the run (incarnation) that member key shows.
CLIENT_NODE = "e2e"
CLIENT_INCARNATION = "e2e-harness-incarnation"


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run docker compose on the harness file."""
    return subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), *args],
        check=check,
        capture_output=True,
        text=True,
        timeout=300,
    )


def container(node: str) -> str:
    """Return the container name of a node."""
    return f"hass-cluster-{node}-1"


def other(node: str) -> str:
    """Return the other node."""
    return NODES[1] if node == NODES[0] else NODES[0]


async def logs(node: str) -> str:
    """Return the log of a node."""
    result = await asyncio.to_thread(compose, "logs", "--no-color", node)
    return result.stdout


async def run(*command: str) -> subprocess.CompletedProcess:
    """Run a command off the event loop."""
    return await asyncio.to_thread(
        subprocess.run, command, check=True, capture_output=True, text=True
    )


async def is_running(node: str) -> bool:
    """Return True while the container of a node runs."""
    result = await asyncio.to_thread(
        subprocess.run,
        ["docker", "inspect", "-f", "{{.State.Running}}", container(node)],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "true"


def start_harness() -> None:
    """Start both nodes and NATS from pristine configs.

    The same steps as ``up.sh``: both nodes get a copy of the template
    (including one shared config entry id, as a replicated ``.storage`` would
    give them).
    """
    compose("down", "-v", "--remove-orphans", check=False)
    shutil.rmtree(RUNTIME, ignore_errors=True)
    for node in NODES:
        shutil.copytree(TEMPLATE, RUNTIME / node)
    compose("up", "-d")


class Cluster:
    """Handle on the running harness."""

    def __init__(self, nc: Any) -> None:
        """Initialize the handle."""
        self.nc = nc
        self.js = nc.jetstream()
        self._renewer: asyncio.Task[None] | None = None

    async def join(self) -> None:
        """Be a member of the cluster (renewed until ``close``), so that calls are accepted."""
        if self._renewer is not None:
            return
        kv = await self.js.key_value("hass_leases")
        payload = json.dumps(
            {
                "protocol": 1,
                "node": CLIENT_NODE,
                "incarnation": CLIENT_INCARNATION,
                "since": None,
                "grace": 0.0,
            }
        ).encode()

        async def renew() -> None:
            while True:
                with suppress(Exception):
                    await kv.put(f"_node.{CLIENT_NODE}", payload)
                await asyncio.sleep(1)

        self._renewer = asyncio.create_task(renew())

    async def close(self) -> None:
        """Stop renewing the member key."""
        if self._renewer is not None:
            self._renewer.cancel()
            with suppress(asyncio.CancelledError):
                await self._renewer

    async def lease_owner(self, key: str) -> str | None:
        """Return the node that holds a key of the lease bucket."""
        try:
            kv = await self.js.key_value("hass_leases")
            entry = await kv.get(key)
        except Exception:  # noqa: BLE001
            return None
        return json.loads(entry.value)["node"]

    async def state(self, entity_id: str) -> dict[str, Any] | None:
        """Return the replicated record of an entity."""
        try:
            kv = await self.js.key_value("hass_states")
            entry = await kv.get(entity_id)
        except Exception:  # noqa: BLE001
            return None
        return json.loads(entry.value)

    async def rpc(
        self,
        node: str,
        domain: str,
        service: str,
        data: dict,
        *,
        role: str = "admin",
    ) -> dict:
        """Call a service through the RPC subject of a node.

        Only the leader runs it; a standby refuses a call that is not a
        broadcast (``ok`` is false).
        """
        msg = await self.nc.request(
            f"hass.rpc.{node}.from.e2e",
            json.dumps(
                {
                    "origin": CLIENT_NODE,
                    "instance": CLIENT_INCARNATION,
                    "role": role,
                    "domain": domain,
                    "service": service,
                    "service_data": data,
                    "blocking": True,
                    "return_response": False,
                    "context": {},
                }
            ).encode(),
            timeout=10,
        )
        return json.loads(msg.data)

    async def wait(self, coro_fn: Any, timeout: float = 60.0) -> Any:
        """Poll until ``coro_fn`` returns something truthy."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if result := await coro_fn():
                return result
            await asyncio.sleep(0.2)
        raise AssertionError(f"timed out after {timeout}s")


@pytest.fixture
async def cluster(request: pytest.FixtureRequest) -> AsyncGenerator[Cluster]:
    """Start the harness, wait until the cluster serves, tear it down after."""
    # Needs Docker, the Home Assistant test image (the nodes run this checkout
    # from a bind mount, nothing is built) and minutes: opt in,
    # so that it is not collected into a run that cannot have either.
    if not os.environ.get("CLUSTER_E2E"):
        pytest.skip(
            "set CLUSTER_E2E=1 (needs Docker and the test image, see harness/up.sh)"
        )
    if not shutil.which("docker"):
        pytest.skip("docker not available")
    # Only inside the Home Assistant test environment: it blocks sockets.
    with suppress(pytest.FixtureLookupError):
        request.getfixturevalue("socket_enabled")
    await asyncio.to_thread(start_harness)
    try:
        nc = None
        for _ in range(60):
            try:
                nc = await nats.connect(
                    NATS_URL, connect_timeout=1, allow_reconnect=False
                )
                break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(1)
        assert nc is not None, "NATS did not come up"
        c = Cluster(nc)

        status: dict[str, bool] = {}
        last_rpc_error: dict[str, Any] = {}

        async def ready() -> bool:
            leader = await c.lease_owner("_leader")
            sun = await c.state(SUN_ENTITY)
            status.update(
                leader=leader is not None,
                members=all([await c.lease_owner(f"_node.{node}") for node in NODES]),
                sun_state=sun is not None and sun["origin"] == leader,
            )
            # The call itself is tried again and again, not part of this check.
            if not all(v for k, v in status.items() if k != "rpc_leader"):
                return False
            await c.join()
            try:
                reply = await c.rpc(
                    str(leader),
                    "input_boolean",
                    "turn_off",
                    {"entity_id": "input_boolean.cluster_probe"},
                )
            except Exception as err:  # noqa: BLE001
                reply = {"error": repr(err)}
            status["rpc_leader"] = bool(reply.get("ok"))
            if not reply.get("ok"):
                last_rpc_error[str(leader)] = reply.get("error")
            return status["rpc_leader"]

        try:
            # A cold start waits lease_ttl + unload_grace before the first
            # leader runs its entries.
            await c.wait(ready, timeout=180)
        except AssertionError as err:
            raise AssertionError(
                f"cluster not ready: {status} rpc errors: {last_rpc_error}"
            ) from err
        yield c
        await c.close()
        await nc.close()
    finally:
        out_dir = HARNESS / "logs"
        out_dir.mkdir(exist_ok=True)
        for node in NODES:
            (out_dir / f"{request.node.name}.{node}.log").write_text(await logs(node))
        await asyncio.to_thread(compose, "down", "-v", "--remove-orphans", check=False)
