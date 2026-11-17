"""Config entry records are encrypted before they reach the NATS server.

Tests against a real NATS JetStream server.
"""

import asyncio
import json
import time
from typing import Any
from unittest.mock import patch

import nats
import pytest

from homeassistant.cluster.const import BUCKET_STORAGE
from homeassistant.cluster.coordinator import ClusterCoordinator
from homeassistant.cluster.crypto import RecordCipher, RecordKeyError
from homeassistant.cluster.lease_manager import HandoverIncomplete
from homeassistant.helpers.discovery_flow import DiscoveryKey

from .conftest import (
    DEV_ENTRY_ID,
    TEST_ENCRYPTION_KEY,
    NodeFactory,
    open_entry_payload,
    start_node,
)
from .test_init import wait_for

OTHER_KEY = "another-key-0123456789abcdefghijklmnopqrstuvw"
SECRET = "secret-token-value-xyz"
KV_KEY = f"entry.{DEV_ENTRY_ID}"


def test_discovery_keys_survive_the_encryption() -> None:
    """What ``ConfigEntry.as_dict`` holds comes back as ``from_json_dict`` reads it."""
    cipher = RecordCipher(TEST_ENCRYPTION_KEY)
    keys = (
        DiscoveryKey(domain="dhcp", key="aa:bb:cc", version=1),
        DiscoveryKey(domain="zeroconf", key=("_x._tcp", "name"), version=2),
    )
    entry = {"data": {"token": SECRET}, "discovery_keys": {"dhcp": keys}}

    opened = cipher.open(KV_KEY, cipher.seal(KV_KEY, entry))

    assert opened["data"] == {"token": SECRET}
    assert (
        tuple(
            DiscoveryKey.from_json_dict(key) for key in opened["discovery_keys"]["dhcp"]
        )
        == keys
    )


async def test_raw_client_cannot_read_the_tokens_of_an_entry(
    make_node: NodeFactory,
) -> None:
    """What review 3 did: read ``entry.<id>`` and the removal announcement."""
    a = await make_node("node_a")
    a.hass.config_entries.async_update_entry(a.entry, data={"token": SECRET})
    await a.hass.async_block_till_done()
    kv = a.cluster.coordinator.kv_storage

    # The new token has been published (it is there once decrypted) ...
    deadline = time.monotonic() + 10
    while True:
        raw = (await kv.get(KV_KEY)).value
        if SECRET in json.dumps(open_entry_payload(KV_KEY, raw)):
            break
        assert time.monotonic() < deadline, "the new token was never published"
        await asyncio.sleep(0.2)
    # ... and a client of the NATS server cannot read it.
    assert SECRET.encode() not in raw
    record = json.loads(raw)
    assert "entry" not in record
    assert record["origin"] == "node_a"  # what routes a record stays readable

    # The same for the announcement of a removal (it carries the whole entry).
    written: list[bytes] = []
    real_put = ClusterCoordinator.async_fenced_put

    async def capture(self: ClusterCoordinator, key: str, value: bytes) -> int:
        written.append(value)
        return await real_put(self, key, value)

    with patch.object(ClusterCoordinator, "async_fenced_put", capture):
        await a.hass.config_entries.async_remove(a.entry.entry_id)

    assert any(b'"removing"' in value for value in written)
    assert all(SECRET.encode() not in value for value in written)
    assert all(b'"entry"' not in value for value in written)


async def test_standby_reads_the_encrypted_record(make_node: NodeFactory) -> None:
    """The cluster works encrypted: the standby gets the leader's data."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    a.hass.config_entries.async_update_entry(a.entry, data={"token": SECRET})

    await wait_for(lambda: b.entry.data.get("token") == SECRET, timeout=15)


async def test_record_of_another_key_blocks_a_promotion(
    make_node: NodeFactory,
) -> None:
    """A node with another key must not lead with its own, older copy."""
    await make_node("node_a")
    b = await make_node("node_b", encryption_key=OTHER_KEY)

    with pytest.raises(HandoverIncomplete, match="every node needs the same"):
        await b.cluster.storage._async_apply_all()


async def test_plain_record_is_refused_when_a_key_is_configured(
    make_node: NodeFactory,
) -> None:
    """Accepting a plain record would be a downgrade."""
    a = await make_node("node_a")
    kv = a.cluster.coordinator.kv_storage
    record = a.entry.as_dict()
    await kv.put(
        "entry.plain_one",
        json.dumps(
            {"origin": "node_x", "entry": {**record, "entry_id": "plain_one"}}
        ).encode(),
    )

    with pytest.raises(HandoverIncomplete, match="plain text"):
        await a.cluster.storage._async_apply_all()


async def test_record_moved_to_another_entry_key_is_refused(
    make_node: NodeFactory,
) -> None:
    """The key of the record is part of what is authenticated."""
    a = await make_node("node_a")
    kv = a.cluster.coordinator.kv_storage
    await a.cluster.storage.async_flush()
    raw = (await kv.get(KV_KEY)).value
    await kv.put("entry.moved_one", raw)

    with pytest.raises(HandoverIncomplete, match="damaged"):
        await a.cluster.storage._async_apply_all()


async def test_encrypted_record_without_a_key_is_refused(
    make_node: NodeFactory,
) -> None:
    """A node without a key cannot read what the others wrote."""
    a = await make_node("node_a")
    await a.cluster.storage.async_flush()
    raw = (await a.cluster.coordinator.kv_storage.get(KV_KEY)).value
    a.cluster.storage._cipher = None

    with pytest.raises(RecordKeyError, match="no encryption_key"):
        a.cluster.storage._open(KV_KEY, json.loads(raw))


def test_cipher_round_trip_and_nonces() -> None:
    """Every seal is new, and opens to the same entry."""
    cipher = RecordCipher(TEST_ENCRYPTION_KEY)
    entry: dict[str, Any] = {"data": {"token": SECRET}, "subentries": [{"x": 1}]}

    first = cipher.seal("entry.a", entry)
    second = cipher.seal("entry.a", entry)

    assert first["n"] != second["n"]
    assert first["ct"] != second["ct"]
    assert SECRET not in json.dumps(first)
    assert cipher.open("entry.a", first) == entry
    assert first["kid"] == cipher.key_id != RecordCipher(OTHER_KEY).key_id


def test_cipher_refuses_what_is_not_its_record() -> None:
    """Another key, another record key, a changed byte, an unknown format."""
    cipher = RecordCipher(TEST_ENCRYPTION_KEY)
    sealed = cipher.seal("entry.a", {"data": {}})

    with pytest.raises(RecordKeyError, match="every node needs"):
        RecordCipher(OTHER_KEY).open("entry.a", sealed)
    with pytest.raises(RecordKeyError, match="damaged"):
        cipher.open("entry.b", sealed)
    tampered = {**sealed, "ct": sealed["ct"][:-4] + "AAAA"}
    with pytest.raises(RecordKeyError, match="damaged"):
        cipher.open("entry.a", tampered)
    with pytest.raises(RecordKeyError, match="format"):
        cipher.open("entry.a", {**sealed, "v": 2})
    # Not a ValueError: a skipped record would leave the node on its own copy.
    assert not issubclass(RecordKeyError, ValueError)


async def test_standby_that_cannot_read_a_record_logs_it_once_per_record_key(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """The watcher of a standby does not repeat itself; a promotion names the keys."""
    a = await make_node("node_a")
    b = await make_node("node_b", encryption_key=OTHER_KEY)
    for token in ("one", "two", "three"):
        a.hass.config_entries.async_update_entry(a.entry, data={"token": token})
        await a.hass.async_block_till_done()
        await a.cluster.storage.async_flush()
        await asyncio.sleep(0.6)

    assert b.entry.data["token"] == "t1"  # never applied
    assert caplog.text.count("Cannot read an entry record") == 1
    # The one line says which keys are involved: the writer's and the reader's.
    key_ids = {RecordCipher(k).key_id for k in (TEST_ENCRYPTION_KEY, OTHER_KEY)}
    line = next(
        line for line in caplog.text.splitlines() if "Cannot read an entry" in line
    )
    assert all(key_id in line for key_id in key_ids)

    with pytest.raises(HandoverIncomplete) as promotion:
        await b.cluster.storage._async_apply_all()
    assert all(key_id in str(promotion.value) for key_id in key_ids)
    assert KV_KEY in str(promotion.value)


async def test_changing_the_key_by_the_documented_procedure(
    make_node: NodeFactory,
) -> None:
    """Stop every node, delete ``hass_storage``, set the new key, start them.

    What the README promises: nothing is lost (each node keeps its own copy of
    the entries), the first leader publishes under the new key, the others
    read it, and the old key can read nothing that is left.
    """
    new_key = "the-new-key-0123456789abcdefghijklmnopqrstuvw"
    a = await make_node("node_a")
    b = await make_node("node_b")
    a.hass.config_entries.async_update_entry(a.entry, data={"token": SECRET})
    await wait_for(lambda: b.entry.data.get("token") == SECRET, timeout=15)
    old_kid = json.loads((await a.cluster.coordinator.kv_storage.get(KV_KEY)).value)[
        "enc"
    ]["kid"]

    # The leader has published its inventory (it remembers that it did).
    await a.cluster.storage.async_flush()
    await wait_for(lambda: a.cluster.storage.memory.seen_inventory, timeout=15)
    # ...and its memory is on disk (it is saved a second after a change; a
    # stop within that second would forget the inventory).
    memory = a.cluster.storage.memory
    await memory._store.async_save(memory._data())

    # 1. Stop every node (like a shutdown: the entries are stopped first).
    for node in (a, b):
        await node.hass.config_entries.async_unload(DEV_ENTRY_ID)
        await node.cluster.async_release()
        await node.cluster.async_close()

    # 2. Delete the bucket on the server (``nats stream rm KV_hass_storage``).
    nc = await nats.connect(make_node.url)
    await nc.jetstream().delete_stream(f"KV_{BUCKET_STORAGE}")
    await nc.close()

    # 3. The new key on every node, 4. start them.
    await start_node(a, make_node.url, encryption_key=new_key)
    await start_node(b, make_node.url, encryption_key=new_key)
    await wait_for(lambda: a.entry.state.value == "loaded", timeout=30)

    assert a.cluster.leases.is_leader
    # The first leader finds the inventory it saw before gone, and says so (the
    # README tells the user to expect it, and that nothing is lost).
    assert a.issue_registry.async_get_issue("homeassistant", "storage_data_lost")
    assert a.entry.data["token"] == SECRET  # nothing was lost
    raw = (await a.cluster.coordinator.kv_storage.get(KV_KEY)).value
    assert SECRET.encode() not in raw
    new_cipher = RecordCipher(new_key)
    record = json.loads(raw)
    assert record["enc"]["kid"] == new_cipher.key_id != old_kid
    assert new_cipher.open(KV_KEY, record["enc"])["data"] == {"token": SECRET}
    with pytest.raises(RecordKeyError):
        RecordCipher(TEST_ENCRYPTION_KEY).open(KV_KEY, record["enc"])

    # The standby reads the new records, and can take over with them.
    a.hass.config_entries.async_update_entry(a.entry, data={"token": "after-change"})
    await wait_for(lambda: b.entry.data.get("token") == "after-change", timeout=15)
    await a.cluster.leases.async_stop(release=True)
    await wait_for(lambda: b.cluster.leases.is_leader, timeout=15)
    assert b.entry.data["token"] == "after-change"
