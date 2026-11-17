"""The protocol version of what cluster nodes write for each other.

Every member key, lease and config entry record carries ``protocol``. A node
only works with its own version: it does not take over while a member of
another version is alive, and it never applies a record of another version.
A key or record without the field was written before the field existed and
counts as version 1, so a cluster that already holds records can be upgraded
without a purge. See the README, "Protocol version".
"""

from typing import Any

from .const import PROTOCOL_VERSION


class UnsupportedProtocol(Exception):
    """A record or member of another protocol version.

    Deliberately not a ``ValueError``: it is not a record with wrong content
    that may be skipped. A node that skipped it would go on to lead with an
    older copy of what the record holds.
    """


def version_of(payload: Any) -> Any:
    """Return the protocol version ``payload`` says it was written with."""
    if isinstance(payload, dict):
        return payload.get("protocol", 1)
    return 1


def is_supported(payload: Any) -> bool:
    """Return True if this node speaks the version of ``payload``.

    Only the integer itself counts: ``True`` and ``1.0`` are not a version.
    """
    version = version_of(payload)
    return type(version) is int and version == PROTOCOL_VERSION


def check_record(kv_key: str, payload: Any) -> None:
    """Raise ``UnsupportedProtocol`` for a record of another version."""
    if not is_supported(payload):
        raise UnsupportedProtocol(
            f"{kv_key} was written with protocol version {version_of(payload)!r},"
            f" this node speaks version {PROTOCOL_VERSION}: run the same version"
            " on every node"
        )
