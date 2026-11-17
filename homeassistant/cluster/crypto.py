"""Encryption of config entry records.

A config entry's ``data`` and ``options`` hold what the entry needs to run:
OAuth tokens, API keys, passwords. They are copied to the NATS server so that
another node can run the entry. The records are encrypted with a key that
every node holds and the NATS server never sees (AES-256-GCM, a new random
nonce for every record). What the cluster needs to route a record (who wrote
it, in which term, which entry and domain, whether it is a removal) stays
readable.

Only config entry records are encrypted. Replicated stores (the registries),
the UI files, the helper states and the entity states are not secrets of that
kind and are published as they are.
"""

import base64
import json
import os
from typing import Any, Final

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from homeassistant.helpers.json import json_bytes

# What a key must at least be: ``python3 -c "import secrets; print(secrets.token_urlsafe(32))"``
# gives 43 characters. A passphrase is refused: the key is not stretched.
MIN_KEY_LENGTH: Final = 32

ENVELOPE_VERSION: Final = 1
_NONCE_BYTES: Final = 12
_KEY_INFO: Final = b"home-assistant-cluster/entry-record/key/v1"
_ID_INFO: Final = b"home-assistant-cluster/entry-record/id/v1"


class RecordKeyError(Exception):
    """An entry record cannot be read with this node's key (or without one).

    Deliberately not a ``ValueError``: a record that cannot be decrypted is not
    a record with wrong content that may be skipped. A node that skipped it
    would go on to lead with its own, older copy of the entry.
    """


def _derive(secret: str, info: bytes) -> bytes:
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=info).derive(
        secret.encode()
    )


_BLOB_KEY_INFO: Final = b"home-assistant-cluster/addon-snapshot/key/v1"


class BlobCipher:
    """Seals and opens add-on snapshots (bytes), with a key of their own.

    The key is derived from the cluster's key with another label than the one
    of entry records, so neither kind of ciphertext opens as the other.
    """

    def __init__(self, secret: str) -> None:
        """Initialize with the cluster's key."""
        self._aead = AESGCM(_derive(secret, _BLOB_KEY_INFO))
        self.key_id = _derive(secret, _ID_INFO)[:6].hex()

    def seal(self, aad: str, data: bytes) -> bytes:
        """Return nonce and ciphertext of ``data``, bound to ``aad``."""
        nonce = os.urandom(_NONCE_BYTES)
        return nonce + self._aead.encrypt(nonce, data, aad.encode())

    def open(self, aad: str, sealed: bytes) -> bytes:
        """Return the plaintext of ``sealed``; raise ``RecordKeyError`` if not ours."""
        try:
            return self._aead.decrypt(
                sealed[:_NONCE_BYTES], sealed[_NONCE_BYTES:], aad.encode()
            )
        except (InvalidTag, ValueError) as err:
            raise RecordKeyError(
                f"{aad} cannot be decrypted with this node's key {self.key_id}"
            ) from err


_BACKUP_PASSWORD_INFO: Final = b"home-assistant-cluster/addon-backup-password/v1"


def backup_password(secret: str) -> str:
    """Return the password of Supervisor backups the cluster keeps on a network storage."""
    return _derive(secret, _BACKUP_PASSWORD_INFO).hex()


class RecordCipher:
    """Seals and opens the entry of an entry record."""

    def __init__(self, secret: str) -> None:
        """Initialize with the cluster's key."""
        self._aead = AESGCM(_derive(secret, _KEY_INFO))
        # Not secret, and not derivable back to the key: tells "another key"
        # from "damaged record".
        self.key_id = _derive(secret, _ID_INFO)[:6].hex()

    def seal(self, kv_key: str, entry: dict[str, Any]) -> dict[str, Any]:
        """Return the envelope of ``entry``, bound to the record key ``kv_key``."""
        nonce = os.urandom(_NONCE_BYTES)
        # The encoder of Core's own JSON files: what ``ConfigEntry.as_dict``
        # holds (the discovery keys are dataclasses) is written in the form
        # ``from_json_dict`` reads back, not as its ``str()``.
        plaintext = json_bytes(entry)
        ciphertext = self._aead.encrypt(nonce, plaintext, kv_key.encode())
        return {
            "v": ENVELOPE_VERSION,
            "kid": self.key_id,
            "n": base64.b64encode(nonce).decode(),
            "ct": base64.b64encode(ciphertext).decode(),
        }

    def open(self, kv_key: str, envelope: Any) -> dict[str, Any]:
        """Return the entry in ``envelope``; raise ``RecordKeyError`` if it is not ours."""
        if not isinstance(envelope, dict) or envelope.get("v") != ENVELOPE_VERSION:
            raise RecordKeyError(
                f"{kv_key} is encrypted in a format this node does not know"
            )
        if (kid := envelope.get("kid")) != self.key_id:
            raise RecordKeyError(
                f"{kv_key} is encrypted with the key {kid}, this node's key is"
                f" {self.key_id}: every node needs the same encryption_key"
            )
        try:
            nonce = base64.b64decode(envelope["n"], validate=True)
            ciphertext = base64.b64decode(envelope["ct"], validate=True)
            plaintext = self._aead.decrypt(nonce, ciphertext, kv_key.encode())
            entry = json.loads(plaintext)
        except (KeyError, TypeError, ValueError, InvalidTag) as err:
            # Same key, but not the record that was written under this key:
            # damaged, or the record of another entry moved to this key.
            raise RecordKeyError(
                f"{kv_key} cannot be decrypted with this node's key {self.key_id}"
                f" ({type(err).__name__}): the record is damaged"
            ) from err
        if not isinstance(entry, dict):
            raise RecordKeyError(f"{kv_key} does not hold a config entry")
        return entry
