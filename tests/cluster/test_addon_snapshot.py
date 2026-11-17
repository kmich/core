"""Add-on snapshots in hass_storage: fenced chunks, against a real NATS server."""

import asyncio
import io
import os
from pathlib import Path
import tarfile
from typing import Any
from unittest.mock import patch

from nats.js.errors import KeyNotFoundError
import pytest

from homeassistant.cluster.addon_gate import SupervisorAddonGate
from homeassistant.cluster.addon_snapshot import (
    PATHS_MEMBER,
    SnapshotError,
    SnapshotStore,
    check_relative_path,
    chunk_key,
    commit_swap,
    manifest_key,
    pack,
    paths_from_options,
    remove_staging,
    rollback_swap,
    swap_in,
    unpack,
)
from homeassistant.cluster.const import TERM_KEY
from homeassistant.cluster.coordinator import TermSuperseded
from homeassistant.cluster.crypto import BlobCipher
from homeassistant.helpers.json import json_bytes
from homeassistant.util.json import json_loads_object

from .conftest import TEST_ENCRYPTION_KEY, NodeFactory, start_node
from .test_addon_gate import FakeSupervisor
from .test_init import wait_for

SLUG = "45df7312_zigbee2mqtt"


def _store(node_coordinator) -> SnapshotStore:
    return SnapshotStore(node_coordinator, BlobCipher(TEST_ENCRYPTION_KEY))


async def _history(kv, key: str) -> int:
    try:
        return len(await kv.history(key))
    except KeyNotFoundError:
        return 0


async def test_snapshot_round_trip_in_chunks(make_node: NodeFactory) -> None:
    """The leader writes a snapshot in chunks; another node reads it back.

    Two slots: each snapshot goes to the slot the manifest does not name, every
    chunk is a rollup (the bucket keeps one value of it), and the chunks of an
    older, longer snapshot in a slot are purged.
    """
    a = await make_node("node_a")
    b = await make_node("node_b")
    writer = _store(a.cluster.coordinator)
    reader = _store(b.cluster.coordinator)
    kv = a.cluster.coordinator.kv_storage
    assert await reader.async_read_manifest(SLUG) is None

    first = os.urandom(100_000)
    with patch.object(SnapshotStore, "_chunk_size", return_value=20_000):
        m1 = await writer.async_write(SLUG, first, {"addon": SLUG})
        assert m1["slot"] == 0
        assert m1["chunks"] == 6  # 100 000 bytes + nonce and tag
        manifest = await reader.async_read_manifest(SLUG)
        assert manifest == m1
        assert await reader.async_read(SLUG, manifest) == first

        second = os.urandom(30_000)
        m2 = await writer.async_write(SLUG, second, {})
        assert (m2["slot"], m2["chunks"], m2["counts"]) == (1, 2, [6, 2])
        assert (
            await reader.async_read(SLUG, await reader.async_read_manifest(SLUG))
            == second
        )

        third = os.urandom(10_000)
        m3 = await writer.async_write(SLUG, third, {})
        assert (m3["slot"], m3["chunks"], m3["counts"]) == (0, 1, [1, 2])
        assert (
            await reader.async_read(SLUG, await reader.async_read_manifest(SLUG))
            == third
        )

    # Slot 0 held six chunks; the five the third snapshot does not use are gone.
    for index in range(1, 6):
        with pytest.raises(KeyNotFoundError):
            await kv.get(chunk_key(SLUG, 0, index))
    # One value per key, whatever the bucket's history.
    assert await _history(kv, chunk_key(SLUG, 0, 0)) == 1
    assert await _history(kv, manifest_key(SLUG)) == 1
    assert m3["node"] == "node_a"
    assert m3["term"] == a.cluster.coordinator.term


async def test_superseded_leader_cannot_write_a_snapshot(
    make_node: NodeFactory,
) -> None:
    """A leader whose term was superseded writes no chunk and no manifest."""
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator
    kv = coordinator.kv_storage
    store = _store(coordinator)
    good = await store.async_write(SLUG, b"current data", {})
    current = await kv.get(TERM_KEY)
    term = json_loads_object(current.value)
    await kv.update(
        TERM_KEY,
        json_bytes({"term": term["term"] + 1, "leader": "node_b"}),
        last=current.revision,
    )

    with pytest.raises(TermSuperseded):
        await store.async_write(SLUG, b"stale data", {})

    assert await store.async_read_manifest(SLUG) == good
    assert await store.async_read(SLUG, good) == b"current data"
    with pytest.raises(KeyNotFoundError):
        await kv.get(chunk_key(SLUG, 1, 0))


async def test_damaged_or_foreign_snapshot_is_refused(make_node: NodeFactory) -> None:
    """A chunk that changed, or another key, is an error and never applied."""
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator
    store = _store(coordinator)
    manifest = await store.async_write(SLUG, b"network key and devices", {})

    other = SnapshotStore(coordinator, BlobCipher("another-key-" + "x" * 32))
    with pytest.raises(SnapshotError, match="every node needs the same"):
        await other.async_read(SLUG, manifest)

    await coordinator.kv_storage.put(chunk_key(SLUG, 0, 0), b"tampered")
    with pytest.raises(SnapshotError, match="does not match its manifest"):
        await store.async_read(SLUG, manifest)


def _tree(root: Path, files: dict[str, bytes]) -> None:
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)


def test_pack_unpack_and_swap(tmp_path: Path) -> None:
    """Folders and the Supervisor backup travel; excluded files and links do not."""
    source = tmp_path / "a"
    _tree(
        source,
        {
            "zigbee2mqtt/configuration.yaml": b"network_key: [1, 2, 3]",
            "zigbee2mqtt/coordinator_backup.json": b"{}",
            "zigbee2mqtt/log/2026-11-24.log": b"x" * 1000,
            "unrelated.yaml": b"not packed",
        },
    )
    (source / "zigbee2mqtt" / "link").symlink_to(source / "unrelated.yaml")
    data = pack(
        b"supervisor tar", source, ["zigbee2mqtt", "missing"], ["zigbee2mqtt/log"]
    )

    target = tmp_path / "b"
    _tree(target, {"zigbee2mqtt/database.db": b"old devices", "keep.yaml": b"kept"})
    unpacked = unpack(data, target, SLUG)
    assert unpacked.supervisor_backup is not None
    assert unpacked.supervisor_backup.read_bytes() == b"supervisor tar"
    commit_swap(swap_in(unpacked, target))
    remove_staging(unpacked)

    restored = sorted(
        p.relative_to(target / "zigbee2mqtt").as_posix()
        for p in (target / "zigbee2mqtt").rglob("*")
    )
    assert restored == ["configuration.yaml", "coordinator_backup.json"]
    assert (target / "keep.yaml").read_bytes() == b"kept"
    assert not (target / ".cluster_addons" / SLUG).exists()
    assert not (target / "zigbee2mqtt.cluster-old").exists()


def test_folder_the_leader_does_not_have_is_removed(tmp_path: Path) -> None:
    """A folder in ``paths`` missing on the snapshot's node goes here too; a file stays."""
    source = tmp_path / "a"
    _tree(source, {"zigbee2mqtt/configuration.yaml": b"new"})
    listed = ["zigbee2mqtt", "z2m_old", "ssl/cert.pem"]
    # Only folders the user configured (``paths``) are removed when absent.
    assert not unpack(pack(None, source, listed, []), tmp_path / "c", SLUG).absent
    data = pack(None, source, listed, [], record_absent=True)

    target = tmp_path / "b"
    _tree(
        target,
        {
            "zigbee2mqtt/configuration.yaml": b"old",
            "z2m_old/database.db": b"stale devices",
            "ssl/cert.pem": b"a file, not add-on data",
        },
    )
    unpacked = unpack(data, target, SLUG)
    assert unpacked.paths == ["zigbee2mqtt"]
    # Neither existed on the leader; only a folder is removed here.
    assert unpacked.absent == ["z2m_old", "ssl/cert.pem"]
    swap = swap_in(unpacked, target)
    assert not (target / "z2m_old").exists()

    # A restore that fails afterwards puts everything back...
    rollback_swap(swap)
    assert (target / "zigbee2mqtt/configuration.yaml").read_bytes() == b"old"
    assert (target / "z2m_old/database.db").read_bytes() == b"stale devices"
    remove_staging(unpacked)

    # ...one that succeeds keeps the snapshot's folders only.
    unpacked = unpack(data, target, SLUG)
    commit_swap(swap_in(unpacked, target))
    remove_staging(unpacked)
    assert (target / "zigbee2mqtt/configuration.yaml").read_bytes() == b"new"
    assert not (target / "z2m_old").exists()
    assert (target / "ssl/cert.pem").read_bytes() == b"a file, not add-on data"
    assert not list(target.glob("*.cluster-old"))


def test_unpack_writes_only_the_configured_folders(tmp_path: Path) -> None:
    """A snapshot that names other places writes nothing there."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for name, content in (
            (f"{PATHS_MEMBER}/zigbee2mqtt/configuration.yaml", b"x"),
            (f"{PATHS_MEMBER}/secrets/secrets.yaml", b"x"),
            (f"{PATHS_MEMBER}/.storage/auth", b"x"),
            ("../escape.txt", b"x"),
            ("paths.json", b'["zigbee2mqtt", ".storage", "../up", "/abs"]'),
        ):
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    unpacked = unpack(buffer.getvalue(), tmp_path, SLUG)
    assert unpacked.paths == ["zigbee2mqtt"]
    commit_swap(swap_in(unpacked, tmp_path))
    remove_staging(unpacked)
    assert (tmp_path / "zigbee2mqtt" / "configuration.yaml").exists()
    assert not (tmp_path / "secrets").exists()
    assert not (tmp_path / ".storage").exists()
    assert not (tmp_path.parent / "escape.txt").exists()
    assert unpacked.supervisor_backup is None


def test_oversized_snapshot_is_refused(tmp_path: Path) -> None:
    """A snapshot above the limit is not made."""
    with (
        patch("homeassistant.cluster.addon_snapshot.MAX_SNAPSHOT_BYTES", 10),
        pytest.raises(SnapshotError, match="exclude logs"),
    ):
        pack(os.urandom(100), tmp_path, [], [])


@pytest.mark.parametrize(
    ("path", "valid"),
    [
        ("zigbee2mqtt", True),
        ("sub/dir", True),
        ("/config/zigbee2mqtt", False),
        ("../share", False),
        ("a/../../b", False),
        ("", False),
        (".cluster_addons", False),
        (".storage", False),
        ("custom_components/x", False),
        (".", False),
    ],
)
def test_relative_paths(path: str, valid: bool) -> None:
    """Only folders inside the configuration folder can be replicated."""
    if valid:
        assert check_relative_path(path) == path
    else:
        with pytest.raises(ValueError):
            check_relative_path(path)


async def test_add_on_follows_the_leader_with_its_data(
    make_node: NodeFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Zigbee2MQTT moves with the leader, with the leader's data.

    Node A is the seed node: it snapshots its data and starts the add-on.
    When it shuts down it stops the add-on, snapshots it stopped and reports
    it stopped; node B then restores that snapshot and starts it. At no time
    do both run it.
    """
    monkeypatch.setenv("SUPERVISOR_TOKEN", "token")
    supervisors = {
        "node_a": FakeSupervisor({SLUG: "stopped"}),
        "node_b": FakeSupervisor({SLUG: "stopped"}),
    }
    overlap: list[str] = []

    async def _request(self: SupervisorAddonGate, *args: Any, **kwargs: Any) -> Any:
        result = await supervisors[self.node_id].request(*args, **kwargs)
        if all(s.states[SLUG] == "started" for s in supervisors.values()):
            overlap.append(f"{self.node_id} {args}")
        return result

    monkeypatch.setattr(SupervisorAddonGate, "_request", _request)
    conf = {
        "managed_addons": [
            {"slug": SLUG, "paths": "zigbee2mqtt", "seed_node": "node_a"}
        ],
        "addon_takeover_wait": 0,
    }
    a = await make_node("node_a", start=False)
    a.hass.config.config_dir = str(tmp_path / "a")
    _tree(tmp_path / "a", {"zigbee2mqtt/configuration.yaml": b"network_key: [7]"})
    await start_node(a, make_node.url, **conf)
    await wait_for(lambda: supervisors["node_a"].states[SLUG] == "started", timeout=15)

    b = await make_node("node_b", start=False)
    b.hass.config.config_dir = str(tmp_path / "b")
    _tree(tmp_path / "b", {"zigbee2mqtt/configuration.yaml": b"network_key: GENERATE"})
    await start_node(b, make_node.url, **conf)
    await wait_for(lambda: supervisors["node_b"].boot[SLUG] == "manual", timeout=15)
    assert supervisors["node_b"].states[SLUG] == "stopped"

    await a.hass.async_stop()
    await wait_for(lambda: supervisors["node_b"].states[SLUG] == "started", timeout=30)

    assert supervisors["node_a"].states[SLUG] == "stopped"
    assert (tmp_path / "b" / "zigbee2mqtt" / "configuration.yaml").read_bytes() == (
        b"network_key: [7]"
    )
    assert "POST /backups/new/upload" in supervisors["node_b"].calls
    assert not overlap
    manifest = await _store(b.cluster.coordinator).async_read_manifest(SLUG)
    assert manifest is not None
    assert manifest["node"] == "node_a"
    assert b.cluster.addon_gate.local_snapshot[SLUG] == manifest["id"]


async def test_handover_outlasts_the_shutdown_job_stage(
    make_node: NodeFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The final snapshot is taken in the stop stage, not in the shutdown jobs.

    Core gives its shutdown jobs one short budget for everything (20 s) and
    cuts them off after it; a Supervisor backup can take longer. Here the
    budget is shorter than the backup: the next leader must still get the
    data the old leader had when it stopped, not the earlier snapshot.
    """
    monkeypatch.setenv("SUPERVISOR_TOKEN", "token")
    monkeypatch.setattr("homeassistant.core.STOPPING_STAGE_SHUTDOWN_TIMEOUT", 0.2)
    supervisors = {
        "node_a": FakeSupervisor({SLUG: "stopped"}),
        "node_b": FakeSupervisor({SLUG: "stopped"}),
    }
    slow = {"backup": False}

    async def _request(self: SupervisorAddonGate, *args: Any, **kwargs: Any) -> Any:
        if slow["backup"] and args[:2] == ("POST", "/backups/new/partial"):
            await asyncio.sleep(0.8)  # longer than the shutdown jobs may take
        return await supervisors[self.node_id].request(*args, **kwargs)

    monkeypatch.setattr(SupervisorAddonGate, "_request", _request)
    conf = {
        "managed_addons": [
            {"slug": SLUG, "paths": "zigbee2mqtt", "seed_node": "node_a"}
        ],
        "addon_takeover_wait": 0,
    }
    a = await make_node("node_a", start=False)
    a.hass.config.config_dir = str(tmp_path / "a")
    _tree(tmp_path / "a", {"zigbee2mqtt/configuration.yaml": b"network_key: [7]"})
    await start_node(a, make_node.url, **conf)
    await wait_for(lambda: supervisors["node_a"].states[SLUG] == "started", timeout=15)
    b = await make_node("node_b", start=False)
    b.hass.config.config_dir = str(tmp_path / "b")
    _tree(tmp_path / "b", {"zigbee2mqtt/configuration.yaml": b"network_key: GENERATE"})
    await start_node(b, make_node.url, **conf)
    await wait_for(lambda: supervisors["node_b"].boot[SLUG] == "manual", timeout=15)
    first = await _store(b.cluster.coordinator).async_read_manifest(SLUG)
    assert first is not None

    # A device paired after the last periodic snapshot.
    _tree(tmp_path / "a", {"zigbee2mqtt/configuration.yaml": b"network_key: [7, 8]"})
    slow["backup"] = True
    await a.hass.async_stop()
    await wait_for(lambda: supervisors["node_b"].states[SLUG] == "started", timeout=30)

    final = await _store(b.cluster.coordinator).async_read_manifest(SLUG)
    assert final is not None
    assert final["id"] != first["id"]
    assert (tmp_path / "b" / "zigbee2mqtt" / "configuration.yaml").read_bytes() == (
        b"network_key: [7, 8]"
    )
    assert supervisors["node_a"].states[SLUG] == "stopped"


async def test_node_without_nats_keeps_the_add_ons_stopped(
    make_node: NodeFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A node that cannot reach NATS at boot stops managed add-ons that still run.

    Home Assistant restarted while its add-ons ran on; without NATS this node
    cannot lead nor see who does, so it must not leave them running while
    another node may take them over.
    """
    monkeypatch.setenv("SUPERVISOR_TOKEN", "token")
    supervisor = FakeSupervisor({SLUG: "started"})

    async def _request(self: SupervisorAddonGate, *args: Any, **kwargs: Any) -> Any:
        return await supervisor.request(*args, **kwargs)

    monkeypatch.setattr(SupervisorAddonGate, "_request", _request)
    a = await make_node("node_a", start=False)
    await start_node(
        a,
        "nats://127.0.0.1:1",
        managed_addons=[{"slug": SLUG, "paths": "zigbee2mqtt"}],
    )
    assert not a.cluster.started
    await wait_for(lambda: supervisor.states[SLUG] == "stopped", timeout=10)
    assert supervisor.boot[SLUG] == "manual"
    assert f"POST /addons/{SLUG}/start" not in supervisor.calls


def test_paths_from_options() -> None:
    """Options that name folders of the configuration folder are found."""
    assert paths_from_options(
        {
            "data_path": "/config/zigbee2mqtt",
            "nested": {
                "db": "/homeassistant/esphome/",
                "list": ["/config/zigbee2mqtt/log"],
            },
            "other": "/share/music",
            "storage": "/config/.storage",
            "number": 5,
        }
    ) == ["zigbee2mqtt", "esphome"]
    assert paths_from_options(None) == []
