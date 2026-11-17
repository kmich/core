"""Snapshots of Supervisor add-ons, kept in ``hass_storage``.

A snapshot holds what a managed add-on needs to run on another host: the
Supervisor backup of the add-on (its ``/data``, its options and its own
``addon_config`` folder) and directories of Home Assistant's configuration
folder that the add-on keeps its data in (Zigbee2MQTT's ``zigbee2mqtt``
folder: the network key, the device database and the coordinator backup).

The snapshot is encrypted with the cluster's key (a key of its own, derived
like the one of entry records) and written in chunks smaller than the NATS
``max_payload``. The writes are fenced like every other write to
``hass_storage``: only the current leader's snapshot is accepted.

Keys: ``addon.<slug>`` is the manifest (which chunks, their digest, who wrote
it in which term); ``addon.<slug>.s<slot>.<n>`` are the chunks. Two slots: a
new snapshot is written to the slot the manifest does not name, and the
manifest is replaced last, so a reader never sees a half-written snapshot.
Chunks are written as rollups: the bucket keeps one value of each.
"""

from dataclasses import dataclass
import fnmatch
import hashlib
import io
from pathlib import Path, PurePosixPath
import shutil
import tarfile
from time import time
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

from nats.js.errors import KeyNotFoundError

from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.json import json_bytes
from homeassistant.util.json import json_loads

from .const import ADDON_KEY_PREFIX
from .coordinator import ClusterCoordinator
from .crypto import BlobCipher

MANIFEST_VERSION: Final = 1
# A snapshot larger than this is refused (before encryption, compressed).
MAX_SNAPSHOT_BYTES: Final = 64 * 1024 * 1024
# Room for the headers of a chunk below the server's max_payload.
_CHUNK_HEADROOM: Final = 4096
_MIN_CHUNK: Final = 16 * 1024

SUPERVISOR_MEMBER: Final = "supervisor.tar"
PATHS_MEMBER: Final = "paths"
# The list of folders in the snapshot (inside the encrypted part: a reader
# takes the folders from here, not from the readable manifest).
PATHS_LIST_MEMBER: Final = "paths.json"
# The configured folders the node that made the snapshot did not have.
ABSENT_LIST_MEMBER: Final = "absent.json"
# The managed add-ons the cluster knows of, and their startup stages.
INDEX_KEY: Final = "addons.index"
# Staging folders in Home Assistant's configuration folder (same file system
# as the paths, so that the swap is a rename).
STAGING_DIR: Final = ".cluster_addons"
_OLD_SUFFIX: Final = ".cluster-old"
# Folders of the configuration folder that belong to Home Assistant itself
# (or are replicated otherwise): never an add-on's data.
PROTECTED_DIRS: Final = frozenset(
    {
        STAGING_DIR,
        ".cloud",
        ".storage",
        "backups",
        "blueprints",
        "custom_components",
        "deps",
        "packages",
        "themes",
        "tts",
        "www",
    }
)


class SnapshotError(HomeAssistantError):
    """A snapshot cannot be made, read or applied."""


def manifest_key(slug: str) -> str:
    """Return the key of an add-on's manifest."""
    return f"{ADDON_KEY_PREFIX}{slug}"


def chunk_key(slug: str, slot: int, index: int) -> str:
    """Return the key of a chunk."""
    return f"{ADDON_KEY_PREFIX}{slug}.s{slot}.{index}"


def check_relative_path(path: str) -> str:
    """Return ``path`` normalized; raise ``ValueError`` unless it stays in the folder."""
    pure = PurePosixPath(path.replace("\\", "/"))
    if not path or pure.is_absolute() or ".." in pure.parts or not pure.parts:
        raise ValueError(
            f"{path!r} must be a folder inside the configuration folder, as a"
            " relative path"
        )
    if pure.parts[0] in PROTECTED_DIRS or pure.parts == (".",):
        raise ValueError(f"{path!r} is a folder of Home Assistant itself")
    return pure.as_posix()


def paths_from_options(options: Any) -> list[str]:
    """Return the folders of the configuration folder an add-on's options name.

    Add-ons that map Home Assistant's configuration folder see it as
    ``/config`` or ``/homeassistant``; an option such as Zigbee2MQTT's
    ``data_path: /config/zigbee2mqtt`` names the folder it keeps its data in.
    Folders of Home Assistant itself are left out; so is a value that is not
    a folder (``pack`` skips what does not exist).
    """
    found: list[str] = []

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            for item in value.values():
                _walk(item)
        elif isinstance(value, list):
            for item in value:
                _walk(item)
        elif isinstance(value, str):
            for prefix in ("/config/", "/homeassistant/"):
                if value.startswith(prefix):
                    try:
                        relative = check_relative_path(
                            value.removeprefix(prefix).rstrip("/")
                        )
                    except ValueError:
                        return
                    if relative not in found:
                        found.append(relative)
                    return

    _walk(options)
    # A folder inside another one travels with it.
    return [
        p for p in found if not any(p != q and p.startswith(f"{q}/") for q in found)
    ]


def _excluded(relative: str, exclude: list[str]) -> bool:
    return any(
        fnmatch.fnmatch(relative, pattern) or relative.startswith(f"{pattern}/")
        for pattern in exclude
    )


def pack(
    supervisor_backup: bytes | None,
    config_dir: Path,
    paths: list[str],
    exclude: list[str],
    record_absent: bool = False,
) -> bytes:
    """Return a snapshot: the Supervisor backup and the folders, as one tar.gz.

    Runs in the executor. A folder that does not exist is left out (an
    add-on that never created it); one that is a symlink is refused. With
    ``record_absent`` (folders the user configured with ``paths``, not ones
    taken from the add-on's options), the snapshot also lists the folders
    that do not exist here, and a restore removes them (``swap_in``).
    """
    try:
        data = _pack(supervisor_backup, config_dir, paths, exclude, record_absent)
    except (tarfile.TarError, OSError) as err:
        raise SnapshotError(f"the snapshot cannot be made: {err}") from err
    if len(data) > MAX_SNAPSHOT_BYTES:
        raise SnapshotError(
            f"the snapshot is {len(data)} bytes, more than the {MAX_SNAPSHOT_BYTES}"
            " a snapshot may have through NATS; exclude logs or other large"
            " files, or set addon_backup_location (a network storage of"
            " Supervisor) for large add-ons"
        )
    return data


def _pack(
    supervisor_backup: bytes | None,
    config_dir: Path,
    paths: list[str],
    exclude: list[str],
    record_absent: bool,
) -> bytes:
    buffer = io.BytesIO()
    packed: list[str] = []
    absent: list[str] = []
    with tarfile.open(fileobj=buffer, mode="w:gz", compresslevel=6) as tar:
        if supervisor_backup is not None:
            info = tarfile.TarInfo(SUPERVISOR_MEMBER)
            info.size = len(supervisor_backup)
            info.mtime = int(time())
            tar.addfile(info, io.BytesIO(supervisor_backup))
        for relative in paths:
            source = config_dir / relative
            if source.is_symlink():
                raise SnapshotError(f"{relative} is a symbolic link")
            if not source.is_dir():
                if record_absent and not source.exists():
                    absent.append(relative)
                continue

            def _filter(
                member: tarfile.TarInfo, relative: str = relative
            ) -> tarfile.TarInfo | None:
                inner = PurePosixPath(member.name).relative_to(
                    PurePosixPath(PATHS_MEMBER) / relative
                )
                name = (PurePosixPath(relative) / inner).as_posix()
                if member.issym() or member.islnk() or _excluded(name, exclude):
                    return None
                return member

            tar.add(
                source,
                arcname=f"{PATHS_MEMBER}/{relative}",
                recursive=True,
                filter=_filter,
            )
            packed.append(relative)
        for member, names in (
            (PATHS_LIST_MEMBER, packed),
            (ABSENT_LIST_MEMBER, absent),
        ):
            listing = json_bytes(names)
            info = tarfile.TarInfo(member)
            info.size = len(listing)
            info.mtime = int(time())
            tar.addfile(info, io.BytesIO(listing))
    return buffer.getvalue()


@dataclass
class Unpacked:
    """A snapshot extracted to a staging folder."""

    staging: Path
    supervisor_backup: Path | None
    # The folders the snapshot has.
    paths: list[str]
    # Folders of the item's ``paths`` the node that made it did not have: a
    # folder there is removed, so no stale copy is left next to the data.
    absent: list[str]


def unpack(data: bytes, config_dir: Path, slug: str) -> Unpacked:
    """Extract a snapshot to a staging folder; runs in the executor.

    Only the folders its list names are taken from it, and only folders that
    may be an add-on's (``check_relative_path``): a snapshot cannot write
    anywhere else, whatever it contains.
    """
    try:
        return _unpack(data, config_dir, slug)
    except (tarfile.TarError, OSError, EOFError) as err:
        shutil.rmtree(config_dir / STAGING_DIR / slug, ignore_errors=True)
        raise SnapshotError(
            f"the snapshot of {slug} cannot be extracted: {err}"
        ) from err


def _unpack(data: bytes, config_dir: Path, slug: str) -> Unpacked:
    staging = config_dir / STAGING_DIR / slug
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        paths = _read_listing(tar, PATHS_LIST_MEMBER)
        absent = [p for p in _read_listing(tar, ABSENT_LIST_MEMBER) if p not in paths]
        members = []
        for member in tar.getmembers():
            name = PurePosixPath(member.name)
            if name.as_posix() == SUPERVISOR_MEMBER or (
                name.parts[:1] == (PATHS_MEMBER,)
                and any(
                    name.parts[1 : 1 + len(PurePosixPath(p).parts)]
                    == PurePosixPath(p).parts
                    for p in paths
                )
            ):
                members.append(member)
        tar.extractall(staging, members=members, filter="data")
    supervisor = staging / SUPERVISOR_MEMBER
    return Unpacked(
        staging,
        supervisor if supervisor.is_file() else None,
        [p for p in paths if (staging / PATHS_MEMBER / p).is_dir()],
        absent,
    )


def _read_listing(tar: tarfile.TarFile, member: str) -> list[str]:
    """Return the folders a list of the snapshot names (only valid ones)."""
    try:
        listing = tar.extractfile(member)
    except KeyError:
        return []
    if listing is None:
        return []
    names = json_loads(listing.read())
    found: list[str] = []
    for listed in names if isinstance(names, list) else []:
        try:
            found.append(check_relative_path(str(listed)))
        except ValueError:
            continue
    return found


@dataclass
class Swap:
    """Folders moved by ``swap_in``: ``(target, the old folder or None)``."""

    moves: list[tuple[Path, Path | None]]


def swap_in(unpacked: Unpacked, config_dir: Path) -> Swap:
    """Put the snapshot's folders in place; runs in the executor.

    Each folder of the snapshot replaces the one here whole: the old one is
    moved aside (a rename on one file system) and the snapshot's moved in. A
    folder the snapshot lists as absent (``Unpacked.absent``) is only moved
    aside. The old folders are kept until ``commit_swap`` (or put back by
    ``rollback_swap``), so a restore of
    the add-on that fails afterwards does not leave new folders next to old
    add-on data. The add-on must be stopped.
    """
    swap = Swap([])
    try:
        for relative in unpacked.paths:
            target = config_dir / relative
            swap.moves.append((target, _move_aside(target)))
            target.parent.mkdir(parents=True, exist_ok=True)
            (unpacked.staging / PATHS_MEMBER / relative).rename(target)
        for relative in unpacked.absent:
            target = config_dir / relative
            # Only a folder: a file of that name is not the add-on's data.
            if target.is_dir() and not target.is_symlink():
                swap.moves.append((target, _move_aside(target)))
    except OSError:
        rollback_swap(swap)
        raise
    return swap


def _move_aside(target: Path) -> Path | None:
    old = target.with_name(target.name + _OLD_SUFFIX)
    shutil.rmtree(old, ignore_errors=True)
    if not target.exists():
        return None
    target.rename(old)
    return old


def commit_swap(swap: Swap) -> None:
    """Remove the old folders; runs in the executor."""
    for _, old in swap.moves:
        if old is not None:
            shutil.rmtree(old, ignore_errors=True)


def rollback_swap(swap: Swap) -> None:
    """Put the old folders back; runs in the executor."""
    for target, old in reversed(swap.moves):
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)
        if old is not None and old.exists():
            old.rename(target)


def remove_staging(unpacked: Unpacked) -> None:
    """Remove the staging folder; runs in the executor."""
    shutil.rmtree(unpacked.staging, ignore_errors=True)


class SnapshotStore:
    """Writes and reads the snapshots of add-ons in ``hass_storage``."""

    def __init__(self, coordinator: ClusterCoordinator, cipher: BlobCipher) -> None:
        """Initialize."""
        self.coordinator = coordinator
        self.cipher = cipher

    def _chunk_size(self) -> int:
        return max(_MIN_CHUNK, self.coordinator.max_payload - _CHUNK_HEADROOM)

    async def async_read_manifest(self, slug: str) -> dict[str, Any] | None:
        """Return the manifest of ``slug``, or None if there is no snapshot."""
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            raise SnapshotError("NATS is not connected")
        try:
            entry = await kv.get(manifest_key(slug))
        except KeyNotFoundError:
            return None
        try:
            manifest = json_loads(entry.value or b"")
        except ValueError as err:
            raise SnapshotError(f"the manifest of {slug} is damaged") from err
        if not isinstance(manifest, dict) or manifest.get("v") != MANIFEST_VERSION:
            raise SnapshotError(
                f"the manifest of {slug} has a format this node does not know"
            )
        return manifest

    async def async_read_index(self) -> dict[str, Any] | None:
        """Return the cluster's add-on index (slug: startup stage), None if none."""
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            raise SnapshotError("NATS is not connected")
        try:
            entry = await kv.get(INDEX_KEY)
        except KeyNotFoundError:
            return None
        try:
            index = json_loads(entry.value or b"")
        except ValueError as err:
            raise SnapshotError("the add-on index is damaged") from err
        addons = index.get("addons") if isinstance(index, dict) else None
        if not isinstance(addons, dict):
            raise SnapshotError("the add-on index is damaged")
        return addons

    async def async_write_index(self, addons: dict[str, Any]) -> None:
        """Replace the cluster's add-on index (fenced)."""
        await self.coordinator.async_fenced_rollup(
            INDEX_KEY, json_bytes({"addons": addons})
        )

    async def async_write(
        self, slug: str, data: bytes, extra: dict[str, Any]
    ) -> dict[str, Any]:
        """Store ``data`` as the snapshot of ``slug``; return the new manifest.

        Raises ``NoTerm`` or ``TermSuperseded`` (fenced writes) when this
        node is not the leader of the current term.
        """
        previous = await self.async_read_manifest(slug)
        counts = [0, 0]
        slot = 0
        if previous is not None:
            old_counts = previous.get("counts")
            if (
                isinstance(old_counts, list)
                and len(old_counts) == 2
                and all(isinstance(c, int) for c in old_counts)
            ):
                counts = list(old_counts)
            slot = 1 - int(previous.get("slot") == 1)
        snapshot_id = uuid4().hex
        sealed = self.cipher.seal(f"{manifest_key(slug)}.{snapshot_id}", data)
        size = self._chunk_size()
        chunks = [sealed[i : i + size] for i in range(0, len(sealed), size)] or [b""]
        for index, chunk in enumerate(chunks):
            await self.coordinator.async_fenced_rollup(
                chunk_key(slug, slot, index), chunk
            )
        # Chunks of an older, longer snapshot in this slot.
        for index in range(len(chunks), counts[slot]):
            await self.coordinator.async_fenced_purge(chunk_key(slug, slot, index))
        counts[slot] = len(chunks)
        manifest = {
            **extra,
            "v": MANIFEST_VERSION,
            "id": snapshot_id,
            "slot": slot,
            "chunks": len(chunks),
            "counts": counts,
            "size": len(sealed),
            "sha256": hashlib.sha256(sealed).hexdigest(),
            "kid": self.cipher.key_id,
            "node": self.coordinator.node_id,
            "term": self.coordinator.term,
            "created": time(),
        }
        await self.coordinator.async_fenced_rollup(
            manifest_key(slug), json_bytes(manifest)
        )
        return manifest

    async def async_read(self, slug: str, manifest: dict[str, Any]) -> bytes:
        """Return the decrypted snapshot that ``manifest`` names."""
        kv = self.coordinator.kv_storage
        if kv is None or not self.coordinator.connected:
            raise SnapshotError("NATS is not connected")
        if TYPE_CHECKING:
            assert kv is not None
        if manifest.get("kid") != self.cipher.key_id:
            raise SnapshotError(
                f"the snapshot of {slug} is encrypted with the key"
                f" {manifest.get('kid')}, this node's key is {self.cipher.key_id}:"
                " every node needs the same encryption_key"
            )
        try:
            slot = int(manifest["slot"])
            count = int(manifest["chunks"])
            snapshot_id = str(manifest["id"])
            digest = str(manifest["sha256"])
        except (KeyError, TypeError, ValueError) as err:
            raise SnapshotError(f"the manifest of {slug} is incomplete") from err
        parts = []
        for index in range(count):
            try:
                entry = await kv.get(chunk_key(slug, slot, index))
            except KeyNotFoundError as err:
                raise SnapshotError(
                    f"chunk {index} of the snapshot of {slug} is missing"
                ) from err
            parts.append(entry.value or b"")
        sealed = b"".join(parts)
        if hashlib.sha256(sealed).hexdigest() != digest:
            raise SnapshotError(
                f"the snapshot of {slug} does not match its manifest (it was"
                " replaced while it was read, or it is damaged)"
            )
        try:
            return self.cipher.open(f"{manifest_key(slug)}.{snapshot_id}", sealed)
        except Exception as err:
            raise SnapshotError(str(err)) from err
