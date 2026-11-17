"""The add-on gate: managed add-ons run on the leader only, with the cluster's data.

Supervisor is a fake that keeps the state of each add-on and records every
request; the snapshot store is an in-memory one (the real one is tested
against NATS in ``test_addon_snapshot.py``).
"""

import asyncio
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
from time import monotonic
from typing import Any
from unittest.mock import patch

from aiohttp import ClientError
import probatio
import pytest
from pytest_homeassistant.test_util.aiohttp import AiohttpClientMocker

from homeassistant.cluster import CONFIG_SCHEMA
from homeassistant.cluster.addon_gate import (
    ManagedAddon,
    SupervisorAddonGate,
    SupervisorError,
    managed_addon_list,
)
from homeassistant.cluster.addon_snapshot import SnapshotError, unpack
from homeassistant.cluster.const import DOMAIN
from homeassistant.core import HomeAssistant
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession

MOSQUITTO = "core_mosquitto"
Z2M = "45df7312_zigbee2mqtt"


class FakeSupervisor:
    """Add-on states and the requests Supervisor received."""

    def __init__(
        self,
        states: dict[str, str],
        *,
        startup: dict[str, str] | None = None,
        options: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        """Initialize with the state of each installed add-on."""
        self.states = dict(states)
        self.boot = dict.fromkeys(states, "auto")
        self.startup = startup or {}
        self.options = options or {}
        self.calls: list[str] = []
        self.bodies: list[tuple[str, Any]] = []
        self.backups: dict[str, bytes] = {}
        self.restored: list[tuple[str, bytes]] = []
        self.refuse: set[str] = set()
        self.start_on_restore = True

    async def request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        data: Any = None,
        timeout: Any = None,
        raw: bool = False,
    ) -> Any:
        """Answer one request like Supervisor does."""
        call = f"{method} {path}"
        self.calls.append(call)
        self.bodies.append((call, json_body))
        if call in self.refuse:
            raise SupervisorError(f"refused {call}")
        parts = path.strip("/").split("/")
        if path == "/addons":
            return {"addons": [{"slug": slug} for slug in self.states]}
        if parts[0] == "addons":
            slug, action = parts[1], parts[2]
            if slug not in self.states:
                raise SupervisorError(f"App {slug} is not installed")
            if action == "info":
                return {
                    "state": self.states[slug],
                    "boot": self.boot[slug],
                    "version": "1",
                    "startup": self.startup.get(slug, "application"),
                    "options": self.options.get(slug, {}),
                    "hostname": slug.replace("_", "-"),
                }
            if action == "start":
                self.states[slug] = "started"
            elif action == "stop":
                self.states[slug] = "stopped"
            elif action == "options":
                self.boot[slug] = json_body["boot"]
            return {}
        if path == "/backups/reload":
            return {}
        if path == "/backups/new/partial":
            (slug,) = json_body["addons"]
            backup = f"b{len(self.calls)}"
            self.backups[backup] = f"backup of {slug}".encode()
            return {"slug": backup}
        if path == "/backups/new/upload":
            backup = f"u{len(self.backups)}"
            self.backups[backup] = data._fields[0][2]
            return {"slug": backup}
        if parts[0] == "backups" and parts[-1] == "download":
            return self.backups[parts[1]]
        if parts[0] == "backups" and parts[-1] == "partial":
            (slug,) = json_body["addons"]
            self.restored.append((slug, self.backups[parts[1]]))
            self.states.setdefault(slug, "stopped")  # a restore installs it
            if self.start_on_restore:
                # Supervisor starts an add-on the backup recorded as started.
                self.states[slug] = "started"
            self.boot[slug] = "auto"
            return {}
        if method == "DELETE":
            self.backups.pop(parts[1], None)
            return {}
        raise AssertionError(f"unexpected request {call}")


class MemoryStore:
    """Snapshots in memory, like ``SnapshotStore``."""

    def __init__(self) -> None:
        """Initialize empty."""
        self.manifests: dict[str, dict[str, Any]] = {}
        self.data: dict[str, bytes] = {}
        self.writes = 0
        self.fail_read: Exception | None = None

    async def async_read_manifest(self, slug: str) -> dict[str, Any] | None:
        """Return the manifest."""
        return self.manifests.get(slug)

    async def async_write(
        self, slug: str, data: bytes, extra: dict[str, Any]
    ) -> dict[str, Any]:
        """Store a snapshot."""
        self.writes += 1
        manifest = {**extra, "id": f"s{self.writes}", "size": len(data), "node": "x"}
        self.manifests[slug] = manifest
        self.data[manifest["id"]] = data
        return manifest

    index: dict[str, Any] | None = None

    async def async_read_index(self) -> dict[str, Any] | None:
        """Return the index."""
        return self.index

    async def async_write_index(self, addons: dict[str, Any]) -> None:
        """Replace the index."""
        self.index = dict(addons)

    async def async_read(self, slug: str, manifest: dict[str, Any]) -> bytes:
        """Return a snapshot."""
        if self.fail_read is not None:
            raise self.fail_read
        return self.data[manifest["id"]]


class FakeLeases:
    """What the gate uses of the lease manager."""

    def __init__(self) -> None:
        """Initialize as the leader, alone."""
        self.is_leader = True
        self.ttl = 0.1
        self.reports: dict[str, tuple[dict[str, Any], float, bool]] = {}
        self.member_extra = None
        self.published = 0
        self.before_exit: list[Any] = []
        self.leader: str | None = None

    def peer_member_reports(self) -> dict[str, tuple[dict[str, Any], float, bool]]:
        """Return the reports of the other nodes."""
        return self.reports

    async def async_publish_member(self) -> None:
        """Count publications."""
        self.published += 1

    def add_before_exit(self, cb: Any) -> None:
        """Remember the watchdog hook."""
        self.before_exit.append(cb)

    def remote_owner(self, key: str) -> str | None:
        """Return the other node seen leading, if any."""
        return self.leader


@pytest.fixture(autouse=True)
def supervisor_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run as if under the Supervisor."""
    monkeypatch.setenv("SUPERVISOR_TOKEN", "token")


@pytest.fixture(autouse=True)
def fast() -> Generator[None]:
    """Short retry and poll intervals."""
    with (
        patch("homeassistant.cluster.addon_gate._RETRY_INTERVAL", 0.05),
        patch("homeassistant.cluster.addon_gate._WAIT_POLL", 0.02),
        patch("homeassistant.cluster.addon_gate._STANDBY_CHECK_INTERVAL", 0.05),
        patch("homeassistant.cluster.addon_gate._RESOLVE_INTERVAL", 0.05),
    ):
        yield


def _gate(
    hass: HomeAssistant,
    supervisor: FakeSupervisor,
    addons: list[ManagedAddon],
    *,
    store: MemoryStore | None = None,
    node_id: str = "node_a",
    takeover_wait: float = 0.0,
    leases: FakeLeases | None = None,
    **kwargs: Any,
) -> tuple[SupervisorAddonGate, FakeLeases, MemoryStore]:
    leases = leases or FakeLeases()
    store = store if store is not None else MemoryStore()
    gate = SupervisorAddonGate(
        hass,
        node_id,
        leases,  # type: ignore[arg-type]
        addons,
        store=store,  # type: ignore[arg-type]
        snapshot_interval=3600,
        takeover_wait=takeover_wait,
        **kwargs,
    )
    gate._request = supervisor.request  # type: ignore[method-assign]
    return gate, leases, store


async def _lead(gate: SupervisorAddonGate) -> None:
    """Make the gate lead, and wait until it started every add-on it manages."""
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    assert gate._lead_task is not None

    async def _all_led() -> None:
        while gate._installed is None or not all(
            gate._state_of(a.slug).led
            for a in gate.addons
            if a.slug not in gate.skip_addons
        ):
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_all_led(), 5)


async def wait_until(condition: Any, timeout: float = 5) -> None:
    """Wait until ``condition()`` is true."""

    async def _wait() -> None:
        while not condition():
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_wait(), timeout)


def _z2m(**kwargs: Any) -> ManagedAddon:
    return ManagedAddon(slug=Z2M, paths=("zigbee2mqtt",), **kwargs)


async def test_seed_node_snapshots_then_starts(hass: HomeAssistant) -> None:
    """Without a snapshot only the seed node starts the add-on, after making one."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    gate, _, store = _gate(hass, supervisor, [_z2m(seed_node="node_a")])
    data_dir = Path(hass.config.config_dir) / "zigbee2mqtt"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "configuration.yaml").write_text("network_key: [1]")

    await _lead(gate)

    assert store.writes == 1
    assert gate.local_snapshot[Z2M] == "s1"
    assert supervisor.states[Z2M] == "started"
    assert supervisor.boot[Z2M] == "manual"
    assert supervisor.calls.index("POST /backups/new/partial") < supervisor.calls.index(
        f"POST /addons/{Z2M}/start"
    )
    # The Supervisor backup does not stay on the host.
    assert not supervisor.backups
    assert gate.member_extra()["addons"] == [Z2M]
    await gate.async_stop()


async def test_no_snapshot_and_not_seed_node_does_not_start(
    hass: HomeAssistant, issue_registry: ir.IssueRegistry
) -> None:
    """A node that is not the seed node never starts an add-on without data."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    gate, _, _ = _gate(hass, supervisor, [_z2m(seed_node="node_b")])
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    await asyncio.sleep(0.2)

    assert f"POST /addons/{Z2M}/start" not in supervisor.calls
    issue = issue_registry.async_get_issue("homeassistant", f"cluster_addon_{Z2M}")
    assert issue is not None
    assert "seed node is node_b" in issue.translation_placeholders["reason"]
    # It is not reported as running.
    assert gate.member_extra()["addons"] == []
    await gate.async_on_leader_change(False)
    await gate.async_stop()


async def test_restore_before_start(hass: HomeAssistant) -> None:
    """A node with older data restores the snapshot, then starts the add-on.

    The add-on is stopped first, the folders are swapped, Supervisor restores
    its part, boot is set to manual again (the restore brings back the
    backup's options), and only then is it started.
    """
    source = FakeSupervisor({Z2M: "started"})
    store = MemoryStore()
    seed, _, _ = _gate(hass, source, [_z2m(seed_node="node_a")], store=store)
    data_dir = Path(hass.config.config_dir) / "zigbee2mqtt"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "configuration.yaml").write_text("network_key: [1, 2]")
    await seed._async_snapshot(_z2m())

    # Node B has stale local data and a running add-on (it was started by hand).
    (data_dir / "configuration.yaml").write_text("network_key: GENERATE")
    supervisor = FakeSupervisor({Z2M: "started"})
    supervisor.start_on_restore = True
    gate, _, _ = _gate(hass, supervisor, [_z2m()], store=store, node_id="node_b")
    await _lead(gate)

    assert (data_dir / "configuration.yaml").read_text() == "network_key: [1, 2]"
    calls = supervisor.calls
    stop = calls.index(f"POST /addons/{Z2M}/stop")
    upload = calls.index("POST /backups/new/upload")
    assert stop < upload
    assert supervisor.restored == [(Z2M, f"backup of {Z2M}".encode())]
    boot = max(i for i, c in enumerate(calls) if c == f"POST /addons/{Z2M}/options")
    assert upload < boot < calls.index(f"POST /addons/{Z2M}/start")
    assert supervisor.boot[Z2M] == "manual"
    assert gate.local_snapshot[Z2M] == "s1"
    await gate.async_stop()


async def test_failed_supervisor_restore_puts_the_folders_back(
    hass: HomeAssistant,
) -> None:
    """If Supervisor cannot restore the add-on, its folders are not swapped either."""
    source = FakeSupervisor({Z2M: "started"})
    store = MemoryStore()
    seed, _, _ = _gate(hass, source, [_z2m(seed_node="node_a")], store=store)
    data_dir = Path(hass.config.config_dir) / "zigbee2mqtt"
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "configuration.yaml").write_text("network_key: [1, 2]")
    await seed._async_snapshot(_z2m())

    (data_dir / "configuration.yaml").write_text("network_key: [9, 9]")
    supervisor = FakeSupervisor({Z2M: "stopped"})
    supervisor.refuse = {"POST /backups/new/upload"}  # a full disk, say
    gate, _, _ = _gate(hass, supervisor, [_z2m()], store=store, node_id="node_b")
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    await asyncio.sleep(0.3)

    assert (data_dir / "configuration.yaml").read_text() == "network_key: [9, 9]"
    assert not (data_dir.parent / "zigbee2mqtt.cluster-old").exists()
    assert f"POST /addons/{Z2M}/start" not in supervisor.calls
    assert Z2M not in gate.local_snapshot  # tried again
    supervisor.refuse = set()
    await wait_until(lambda: supervisor.states[Z2M] == "started")
    assert (data_dir / "configuration.yaml").read_text() == "network_key: [1, 2]"
    await gate.async_on_leader_change(False)
    await gate.async_stop()


async def test_current_data_is_not_restored(hass: HomeAssistant) -> None:
    """A node whose data is the latest snapshot starts without a restore."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    gate, _, store = _gate(hass, supervisor, [_z2m()])
    store.manifests[Z2M] = {"id": "s7"}
    gate.local_snapshot[Z2M] = "s7"
    await _lead(gate)
    assert "POST /backups/new/upload" not in supervisor.calls
    assert supervisor.states[Z2M] == "started"
    await gate.async_stop()


async def test_failed_restore_is_retried_and_never_starts(
    hass: HomeAssistant, issue_registry: ir.IssueRegistry
) -> None:
    """A snapshot that cannot be read leaves the add-on stopped, with a repair."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    gate, _, store = _gate(hass, supervisor, [_z2m()])
    store.manifests[Z2M] = {"id": "s1"}
    store.fail_read = SnapshotError("chunk 0 of the snapshot is missing")
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    await asyncio.sleep(0.3)
    assert f"POST /addons/{Z2M}/start" not in supervisor.calls
    assert issue_registry.async_get_issue("homeassistant", f"cluster_addon_{Z2M}")

    store.fail_read = None
    store.data["s1"] = b"not a tar"  # unreadable: still never started
    await asyncio.sleep(0.2)
    assert f"POST /addons/{Z2M}/start" not in supervisor.calls
    await gate.async_on_leader_change(False)
    await gate.async_stop()


async def test_waits_while_another_node_may_run_it(hass: HomeAssistant) -> None:
    """A live node that reports the add-on, or one that just went silent, blocks the start."""
    supervisor = FakeSupervisor({MOSQUITTO: "stopped"})
    gate, leases, _ = _gate(
        hass, supervisor, [ManagedAddon(MOSQUITTO, replicate=False)], takeover_wait=60
    )
    gate._started_at = monotonic() - 1000
    now = monotonic()
    leases.reports = {"node_b": ({"addons": [MOSQUITTO]}, now, True)}
    assert "may still run on node node_b" in gate.blocked_by(MOSQUITTO)
    leases.reports = {"node_b": ({"addons": [MOSQUITTO]}, now - 10, False)}
    assert "has not reported for 10s" in gate.blocked_by(MOSQUITTO)
    leases.reports = {"node_b": ({"addons": [MOSQUITTO]}, now - 61, False)}
    assert gate.blocked_by(MOSQUITTO) is None
    leases.reports = {"node_b": ({"addons": []}, now, True)}
    assert gate.blocked_by(MOSQUITTO) is None
    # Nodes that went away before this one started never reported here.
    gate._started_at = monotonic()
    assert "started recently" in gate.blocked_by(MOSQUITTO)


async def test_start_follows_the_other_node_stopping(hass: HomeAssistant) -> None:
    """The add-on starts once the other node reports it stopped."""
    supervisor = FakeSupervisor({MOSQUITTO: "stopped"})
    gate, leases, _ = _gate(
        hass, supervisor, [ManagedAddon(MOSQUITTO, replicate=False)], takeover_wait=60
    )
    leases.reports = {"node_b": ({"addons": [MOSQUITTO]}, monotonic(), True)}
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    await asyncio.sleep(0.1)
    assert supervisor.states[MOSQUITTO] == "stopped"
    leases.reports = {"node_b": ({"addons": []}, monotonic(), True)}
    await wait_until(lambda: supervisor.states[MOSQUITTO] == "started")
    await gate.async_stop()


async def test_order_and_skip(hass: HomeAssistant) -> None:
    """Add-ons start in the configured order; a skipped one never starts here."""
    supervisor = FakeSupervisor({"a": "stopped", "b": "stopped", "c": "stopped"})
    gate, _, _ = _gate(
        hass, supervisor, [ManagedAddon(s, replicate=False) for s in ("a", "b", "c")]
    )
    gate.skip_addons = frozenset({"b"})
    await _lead(gate)
    starts = [c for c in supervisor.calls if c.endswith("/start")]
    assert starts == ["POST /addons/a/start", "POST /addons/c/start"]
    await gate.async_stop()


async def test_standby_stops_and_sets_manual_boot(hass: HomeAssistant) -> None:
    """A standby stops managed add-ons in reverse order and reports them stopped."""
    supervisor = FakeSupervisor({MOSQUITTO: "started", Z2M: "started"})
    gate, leases, _ = _gate(hass, supervisor, [ManagedAddon(MOSQUITTO), _z2m()])
    assert gate.member_extra()["addons"] == sorted([MOSQUITTO, Z2M])

    await gate.async_enforce_standby()

    stops = [c for c in supervisor.calls if c.endswith("/stop")]
    assert stops == [f"POST /addons/{Z2M}/stop", f"POST /addons/{MOSQUITTO}/stop"]
    assert supervisor.boot == {MOSQUITTO: "manual", Z2M: "manual"}
    assert gate.member_extra()["addons"] == []
    assert leases.published == 1


async def test_standby_keeps_reporting_what_it_could_not_stop(
    hass: HomeAssistant,
) -> None:
    """An add-on Supervisor would not stop is still reported: the leader waits."""
    supervisor = FakeSupervisor({Z2M: "started"})
    supervisor.refuse.add(f"POST /addons/{Z2M}/stop")
    gate, _, _ = _gate(hass, supervisor, [_z2m()])
    await gate.async_enforce_standby()
    assert gate.member_extra()["addons"] == [Z2M]


async def test_standby_loop_catches_a_later_start(hass: HomeAssistant) -> None:
    """An add-on started on a standby after the first check is stopped again."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    gate, leases, _ = _gate(hass, supervisor, [_z2m()])
    leases.is_leader = False
    await gate.async_start()
    assert leases.member_extra == gate.member_extra
    await asyncio.sleep(0.4)
    supervisor.states[Z2M] = "started"  # started by hand in the standby's UI
    await asyncio.sleep(0.3)
    assert supervisor.states[Z2M] == "stopped"
    await gate.async_stop()


@pytest.mark.parametrize("leader", [None, "node_b"])
async def test_standby_boot_waits_only_while_nobody_leads(
    hass: HomeAssistant, leader: str | None
) -> None:
    """A booting standby holds off for an election, not when a leader exists."""
    supervisor = FakeSupervisor({Z2M: "started"})
    leases = FakeLeases()
    leases.is_leader = False
    leases.ttl = 30.0  # the wait would be a minute
    leases.leader = leader
    gate, _, _ = _gate(hass, supervisor, [_z2m()], leases=leases)
    await gate.async_start()
    await asyncio.sleep(0.6)
    assert supervisor.states[Z2M] == ("stopped" if leader else "started")
    await gate.async_stop()


async def test_demotion_stops_the_add_ons(hass: HomeAssistant) -> None:
    """A leader that stops leading stops its add-ons at once."""
    supervisor = FakeSupervisor({MOSQUITTO: "stopped"})
    gate, _, _ = _gate(hass, supervisor, [ManagedAddon(MOSQUITTO, replicate=False)])
    await _lead(gate)
    assert supervisor.states[MOSQUITTO] == "started"
    await gate.async_on_leader_change(False)
    await asyncio.sleep(0.1)
    assert supervisor.states[MOSQUITTO] == "stopped"
    assert gate.member_extra()["addons"] == []
    await gate.async_stop()


async def test_teardown_while_leading_stops_the_add_ons(hass: HomeAssistant) -> None:
    """A node that tears the cluster down while it leads stops its add-ons."""
    supervisor = FakeSupervisor({MOSQUITTO: "stopped"})
    gate, _, _ = _gate(hass, supervisor, [ManagedAddon(MOSQUITTO, replicate=False)])
    await _lead(gate)
    assert supervisor.states[MOSQUITTO] == "started"
    await gate.async_stop()
    assert supervisor.states[MOSQUITTO] == "stopped"


async def test_handover_stops_then_snapshots(hass: HomeAssistant) -> None:
    """At a planned handover the leader snapshots its add-ons stopped."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    gate, _, store = _gate(hass, supervisor, [_z2m(seed_node="node_a")])
    await _lead(gate)
    assert store.writes == 1

    await gate.async_handover()

    assert store.writes == 2
    calls = supervisor.calls
    last_stop = max(i for i, c in enumerate(calls) if c == f"POST /addons/{Z2M}/stop")
    last_backup = max(
        i for i, c in enumerate(calls) if c == "POST /backups/new/partial"
    )
    assert last_stop < last_backup
    assert gate.member_extra()["addons"] == []
    await gate.async_stop()


@pytest.mark.usefixtures("socket_enabled")
async def test_watchdog_stops_add_ons_from_a_detached_process(
    hass: HomeAssistant,
) -> None:
    """The watchdog's hook returns at once; a helper process stops the add-ons."""
    supervisor = FakeSupervisor({Z2M: "started", MOSQUITTO: "started"})
    gate, leases, _ = _gate(hass, supervisor, [ManagedAddon(MOSQUITTO), _z2m()])
    await gate.async_start()
    assert leases.before_exit == [gate.stop_on_exit]
    gate.maybe_running = {MOSQUITTO, Z2M}
    gate.member_extra()  # as each renewal does: the copy for the thread
    received: list[tuple[str, str | None]] = []
    done = threading.Event()

    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            received.append((self.path, self.headers.get("Authorization")))
            self.send_response(200 if "mosquitto" not in self.path else 500)
            self.end_headers()
            if len(received) == 2:
                done.set()

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}"
        with patch("homeassistant.cluster.addon_gate.SUPERVISOR_URL", url):
            started = monotonic()
            gate.stop_on_exit()
            assert monotonic() - started < 0.05 * 4  # spawned, not waited for
        assert await hass.async_add_executor_job(done.wait, 30)
    finally:
        server.shutdown()
    assert received == [
        (f"/addons/{Z2M}/stop", "Bearer token"),
        (f"/addons/{MOSQUITTO}/stop", "Bearer token"),
    ]
    gate.maybe_running = set()
    gate.member_extra()
    with patch("subprocess.Popen") as popen:
        gate.stop_on_exit()
    popen.assert_not_called()
    await gate.async_stop()


async def test_nothing_happens_outside_the_supervisor(
    hass: HomeAssistant, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without a Supervisor token no request is made and nothing is reported."""
    monkeypatch.delenv("SUPERVISOR_TOKEN")
    supervisor = FakeSupervisor({Z2M: "started"})
    gate, leases, _ = _gate(hass, supervisor, [_z2m()])
    await gate.async_start()
    await gate.async_on_leader_change(True)
    assert not supervisor.calls
    assert gate.member_extra() == {}
    assert leases.member_extra is None


@pytest.mark.parametrize(
    ("status", "body", "error"),
    [
        pytest.param(
            200, {"result": "ok", "data": {"state": "started"}}, None, id="ok"
        ),
        pytest.param(
            500, {"result": "error", "message": "boom"}, "HTTP 500: boom", id="http"
        ),
        pytest.param(200, {"result": "error", "message": "no"}, "no", id="result"),
    ],
)
async def test_request_checks_the_answer(
    hass: HomeAssistant,
    aioclient_mock: AiohttpClientMocker,
    status: int,
    body: dict[str, Any],
    error: str | None,
) -> None:
    """A request counts as done only if Supervisor accepted it."""
    aioclient_mock.get("http://supervisor/addons/a/info", status=status, json=body)
    async_get_clientsession(hass)
    gate = SupervisorAddonGate(
        hass,
        "node_a",
        FakeLeases(),
        [],
        snapshot_interval=60,
        takeover_wait=0,  # type: ignore[arg-type]
    )
    if error is None:
        assert await gate._info("a") == {"state": "started"}
    else:
        with pytest.raises(SupervisorError, match=error):
            await gate._info("a")
    assert aioclient_mock.mock_calls[0][3]["Authorization"] == "Bearer token"


async def test_unreachable_supervisor_is_an_error(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    """A Supervisor that cannot be reached raises, and a start/stop call says so."""
    aioclient_mock.post("http://supervisor/addons/a/start", exc=ClientError("down"))
    gate = SupervisorAddonGate(
        hass,
        "node_a",
        FakeLeases(),
        [],
        snapshot_interval=60,
        takeover_wait=0,  # type: ignore[arg-type]
    )
    assert not await gate._call("a", "start")


def _schema(managed: list[Any], **extra: Any) -> dict[str, Any]:
    return CONFIG_SCHEMA(
        {
            DOMAIN: {
                "node_id": "node_a",
                "security": "permissive",
                "managed_addons": managed,
                **extra,
            }
        }
    )[DOMAIN]


def test_schema_of_managed_addons() -> None:
    """Strings and mappings are accepted; replicated add-ons need the key."""
    conf = _schema(
        [
            MOSQUITTO,
            {
                "slug": Z2M,
                "paths": "zigbee2mqtt",
                "exclude": ["zigbee2mqtt/log"],
                "seed_node": "node_a",
            },
            {"slug": "a0d7b954_vscode", "replicate": False},
        ],
        encryption_key="k" * 32,
    )
    assert managed_addon_list(conf["managed_addons"]) == (
        [
            ManagedAddon(MOSQUITTO),
            ManagedAddon(Z2M, ("zigbee2mqtt",), ("zigbee2mqtt/log",), True, "node_a"),
            ManagedAddon("a0d7b954_vscode", replicate=False),
        ],
        False,
    )
    conf = _schema(
        ["all", {"slug": Z2M, "exclude": "zigbee2mqtt/log"}], encryption_key="k" * 32
    )
    assert managed_addon_list(conf["managed_addons"]) == (
        [ManagedAddon(Z2M, None, ("zigbee2mqtt/log",))],
        True,
    )
    with pytest.raises(probatio.Invalid, match="all are snapshotted"):
        _schema("all")
    with pytest.raises(probatio.Invalid):
        _schema([{"slug": Z2M, "paths": ".storage"}], encryption_key="k" * 32)
    assert _schema([{"slug": "x", "replicate": False}])["managed_addons"]
    with pytest.raises(probatio.Invalid, match="configure encryption_key"):
        _schema([MOSQUITTO])
    with pytest.raises(probatio.Invalid):
        _schema(
            [{"slug": Z2M, "paths": "/config/zigbee2mqtt"}], encryption_key="k" * 32
        )
    with pytest.raises(probatio.Invalid):
        _schema(["Not A Slug"], encryption_key="k" * 32)


async def test_all_manages_every_installed_add_on_in_stage_order(
    hass: HomeAssistant,
) -> None:
    """With all, every installed add-on but the local ones starts, by stage."""
    supervisor = FakeSupervisor(
        {
            "music_assistant": "stopped",
            Z2M: "stopped",
            MOSQUITTO: "stopped",
            "core_ssh": "started",
        },
        startup={
            MOSQUITTO: "system",
            Z2M: "application",
            "music_assistant": "application",
        },
    )
    gate, _, store = _gate(
        hass,
        supervisor,
        [],
        manage_all=True,
        local_addons=frozenset({"core_ssh"}),
        seed_node="node_a",
    )
    await _lead(gate)
    starts = [c for c in supervisor.calls if c.endswith("/start")]
    # Mosquitto (system) before the application stage; then by name.
    assert starts == [
        f"POST /addons/{MOSQUITTO}/start",
        f"POST /addons/{Z2M}/start",
        "POST /addons/music_assistant/start",
    ]
    assert "POST /addons/core_ssh/stop" not in supervisor.calls
    assert set(store.manifests) == {MOSQUITTO, Z2M, "music_assistant"}
    assert store.index == {
        MOSQUITTO: {"stage": "system", "held_by": None},
        Z2M: {"stage": "application", "held_by": None},
        "music_assistant": {"stage": "application", "held_by": None},
    }
    await gate.async_stop()
    # A local add-on is never touched, also not on teardown.
    assert supervisor.states["core_ssh"] == "started"


async def test_standby_with_all_stops_everything_but_local(hass: HomeAssistant) -> None:
    """A standby with all stops every installed add-on except the local ones."""
    supervisor = FakeSupervisor(
        {
            "music_assistant": "started",
            "core_ssh": "started",
            "a0d7b954_nodered": "started",
        }
    )
    gate, _, _ = _gate(
        hass, supervisor, [], manage_all=True, local_addons=frozenset({"core_ssh"})
    )
    await gate.async_resolve()
    await gate.async_enforce_standby()
    assert supervisor.states == {
        "music_assistant": "stopped",
        "core_ssh": "started",
        "a0d7b954_nodered": "stopped",
    }
    assert gate.member_extra()["addons"] == []


async def test_paths_come_from_the_add_on_options(hass: HomeAssistant) -> None:
    """Without paths, the folders the add-on's options name are snapshotted."""
    supervisor = FakeSupervisor(
        {Z2M: "stopped"},
        options={
            Z2M: {"data_path": "/config/zigbee2mqtt", "serial": {"port": "tcp://x"}}
        },
    )
    config_dir = Path(hass.config.config_dir)
    (config_dir / "zigbee2mqtt").mkdir(parents=True, exist_ok=True)
    (config_dir / "zigbee2mqtt" / "database.db").write_text("devices")
    gate, _, store = _gate(hass, supervisor, [ManagedAddon(Z2M, seed_node="node_a")])
    await _lead(gate)
    await gate.async_stop()

    unpacked = unpack(store.data["s1"], Path(hass.config.config_dir) / "restored", Z2M)
    assert unpacked.paths == ["zigbee2mqtt"]
    assert (unpacked.staging / "paths" / "zigbee2mqtt" / "database.db").read_text() == (
        "devices"
    )


async def test_new_add_on_after_the_first_start_is_seeded_by_the_leader(
    hass: HomeAssistant,
) -> None:
    """An add-on installed after the cluster knew its add-ons needs no seed node."""
    supervisor = FakeSupervisor({"music_assistant": "started"})
    store = MemoryStore()
    store.index = {MOSQUITTO: "system"}  # the cluster knew Mosquitto only
    gate, _, _ = _gate(hass, supervisor, [], store=store, manage_all=True)
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    await wait_until(lambda: "music_assistant" in store.manifests)
    # Mosquitto is in the index but has no snapshot: not seeded from here,
    # and not reported as running (it is not installed here).
    assert MOSQUITTO not in store.manifests
    await wait_until(lambda: MOSQUITTO not in gate.member_extra()["addons"])
    await gate.async_on_leader_change(False)
    await gate.async_stop()
    assert store.index == {
        MOSQUITTO: "system",
        "music_assistant": {"stage": "application", "held_by": None},
    }


async def test_first_start_without_seed_node_starts_nothing(
    hass: HomeAssistant, issue_registry: ir.IssueRegistry
) -> None:
    """With no index (the first start) and no seed node, nothing is seeded."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    gate, _, store = _gate(hass, supervisor, [], manage_all=True)
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    await asyncio.sleep(0.3)
    assert f"POST /addons/{Z2M}/start" not in supervisor.calls
    assert not store.manifests
    issue = issue_registry.async_get_issue("homeassistant", f"cluster_addon_{Z2M}")
    assert issue is not None
    assert "addon_seed_node" in issue.translation_placeholders["reason"]
    await gate.async_on_leader_change(False)
    await gate.async_stop()


async def test_one_stuck_add_on_does_not_hold_up_the_others(
    hass: HomeAssistant,
) -> None:
    """An add-on that cannot start is retried; the ones after it start anyway."""
    supervisor = FakeSupervisor(
        {MOSQUITTO: "stopped", "music_assistant": "stopped"},
        startup={MOSQUITTO: "system"},
    )
    store = MemoryStore()
    store.manifests[MOSQUITTO] = {"id": "s9"}
    store.fail_read = SnapshotError("chunk 0 of the snapshot is missing")
    gate, _, _ = _gate(
        hass,
        supervisor,
        [ManagedAddon(MOSQUITTO), ManagedAddon("music_assistant", replicate=False)],
        store=store,
    )
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    await wait_until(lambda: supervisor.states["music_assistant"] == "started")
    assert supervisor.states[MOSQUITTO] == "stopped"
    await gate.async_on_leader_change(False)
    await gate.async_stop()


async def test_large_add_ons_go_through_the_network_storage(
    hass: HomeAssistant,
) -> None:
    """With a backup location the Supervisor backup stays there, password-protected.

    NATS holds the manifest (which backup) and the folders. The backup before
    the previous one is deleted; a new leader restores from the location.
    """
    supervisor = FakeSupervisor({"core_mariadb": "stopped"})
    gate, _, store = _gate(
        hass,
        supervisor,
        [ManagedAddon("core_mariadb", seed_node="node_a")],
        backup_location="nas",
        backup_password="secret",
    )
    await _lead(gate)
    await gate.async_snapshot_led()
    await gate.async_snapshot_led()
    await gate.async_stop()

    partials = [b for c, b in supervisor.bodies if c == "POST /backups/new/partial"]
    assert len(partials) == 3
    assert all(b["location"] == "nas" and b["password"] == "secret" for b in partials)
    assert not any(c.endswith("/download") for c in supervisor.calls)
    manifest = store.manifests["core_mariadb"]
    deletes = [(c, b) for c, b in supervisor.bodies if c.startswith("DELETE")]
    assert len(deletes) == 1
    assert deletes[0][1] == {"location": "nas"}
    assert manifest["location"] == "nas"
    assert manifest["previous_backup"] in supervisor.backups

    restorer = FakeSupervisor({"core_mariadb": "stopped"})
    other, _, _ = _gate(
        hass,
        restorer,
        [ManagedAddon("core_mariadb")],
        store=store,
        node_id="node_b",
        backup_location="nas",
        backup_password="secret",
    )
    restorer.backups[manifest["backup"]] = b"on the nas"
    await _lead(other)
    await other.async_stop()
    assert "POST /backups/reload" in restorer.calls
    restore = next(b for c, b in restorer.bodies if c.endswith("/restore/partial"))
    assert restore == {
        "addons": ["core_mariadb"],
        "background": False,
        "location": "nas",
        "password": "secret",
    }
    assert "POST /backups/new/upload" not in restorer.calls


async def test_data_on_another_node_is_never_replaced_by_an_empty_install(
    hass: HomeAssistant, issue_registry: ir.IssueRegistry
) -> None:
    """An add-on only another node has (without a snapshot) is not seeded here.

    The leader records which node holds its data and raises the repair issue
    naming it. Installing the add-on here later does not change that: the
    empty install would replace the other node's data (for Zigbee2MQTT: form
    a new network).
    """
    supervisor = FakeSupervisor({MOSQUITTO: "stopped"})
    store = MemoryStore()
    leases = FakeLeases()
    leases.reports = {
        "node_b": ({"addons": [], "installed": [MOSQUITTO, Z2M]}, monotonic(), True)
    }
    gate, _, _ = _gate(
        hass,
        supervisor,
        [],
        store=store,
        leases=leases,
        manage_all=True,
        seed_node="node_a",
    )
    gate.started = True
    gate._started_at = monotonic() - 1000
    await gate.async_on_leader_change(True)
    await wait_until(lambda: supervisor.states[MOSQUITTO] == "started")
    await wait_until(
        lambda: (store.index or {}).get(Z2M, {}).get("held_by") == "node_b"
    )
    await wait_until(
        lambda: (
            issue_registry.async_get_issue("homeassistant", f"cluster_addon_{Z2M}")
            is not None
        )
    )
    issue = issue_registry.async_get_issue("homeassistant", f"cluster_addon_{Z2M}")
    assert "let node node_b lead once" in issue.translation_placeholders["reason"]

    # Installed here now, empty: still not seeded, not started.
    supervisor.states[Z2M] = "stopped"
    supervisor.boot[Z2M] = "auto"
    await asyncio.sleep(0.3)
    assert Z2M not in store.manifests
    assert f"POST /addons/{Z2M}/start" not in supervisor.calls
    await gate.async_on_leader_change(False)
    await gate.async_stop()


async def test_the_node_holding_the_data_seeds_it(hass: HomeAssistant) -> None:
    """The node the index names as holder seeds the add-on when it leads."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    store = MemoryStore()
    store.index = {Z2M: {"stage": "application", "held_by": "node_b"}}
    gate, _, _ = _gate(
        hass,
        supervisor,
        [],
        store=store,
        node_id="node_b",
        manage_all=True,
        seed_node="node_a",
    )
    await _lead(gate)
    await gate.async_stop()
    assert Z2M in store.manifests
    assert store.index[Z2M] == {"stage": "application", "held_by": None}


async def test_installed_on_two_nodes_needs_a_seed_node(hass: HomeAssistant) -> None:
    """New to the cluster but installed on two nodes: which copy is right is unknown."""
    supervisor = FakeSupervisor({Z2M: "stopped"})
    store = MemoryStore()
    store.index = {MOSQUITTO: {"stage": "system", "held_by": None}}
    leases = FakeLeases()
    leases.reports = {
        "node_b": ({"addons": [], "installed": [Z2M]}, monotonic(), False)
    }
    gate, _, _ = _gate(
        hass, supervisor, [], store=store, leases=leases, manage_all=True
    )
    await gate.async_resolve()
    assert "node node_b has it installed too" in gate.seed_blocker(ManagedAddon(Z2M))
    leases.reports = {}
    assert gate.seed_blocker(ManagedAddon(Z2M)) is None


async def test_the_clusters_own_nats_add_on_stays_local(hass: HomeAssistant) -> None:
    """An add-on whose host name is a NATS server of the cluster is never stopped."""
    supervisor = FakeSupervisor(
        {"a1b2c3d4_nats": "started", "music_assistant": "started"}
    )
    gate, _, _ = _gate(
        hass,
        supervisor,
        [],
        manage_all=True,
        nats_hosts=frozenset({"a1b2c3d4-nats"}),
    )
    await gate.async_resolve()
    await gate.async_enforce_standby()
    assert supervisor.states == {
        "a1b2c3d4_nats": "started",
        "music_assistant": "stopped",
    }
    assert gate.member_extra() == {"addons": [], "installed": ["music_assistant"]}
