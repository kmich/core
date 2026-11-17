"""Removals and token refreshes survive a crash or a lost bucket.

Against a real NATS JetStream server (section 3 of the open review items).
"""

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import nats
from nats.js.errors import KeyNotFoundError
import pytest

from homeassistant.cluster.const import BUCKET_STORAGE
from homeassistant.cluster.coordinator import ClusterCoordinator
from homeassistant.cluster.durability import (
    ISSUE_DATA_LOST,
    ISSUE_DOMAIN,
    ISSUE_MISSING_CREDENTIAL,
    ISSUE_TOKEN_LOST,
)
from homeassistant.cluster.lease_manager import LeaseManager
from homeassistant.cluster.storage_sync import INVENTORY_KEY
from homeassistant.config_entries import ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util.json import json_loads_object

from .conftest import (
    DEV_ENTRY_ID,
    HardExit,
    NodeFactory,
    open_entry_payload,
    start_node,
)
from .tcp_proxy import BlackholeProxy
from .test_failure_modes import install_custom
from .test_init import wait_for

LOADED = ConfigEntryState.LOADED
NOT_LOADED = ConfigEntryState.NOT_LOADED


async def _record(node_coordinator: ClusterCoordinator, key: str) -> dict | None:
    kv = node_coordinator.kv_storage
    assert kv is not None
    try:
        raw = (await kv.get(key)).value
    except KeyNotFoundError:
        return None
    return (
        open_entry_payload(key, raw)
        if key.startswith("entry.")
        else json_loads_object(raw)
    )


async def test_removal_is_announced_before_the_clean_up(
    make_node: NodeFactory,
) -> None:
    """The record says ``removing`` while the clean-up runs, then ``removed``."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    seen_during_cleanup: list[dict | None] = []
    original = ConfigEntry.async_remove

    async def _remove(self: ConfigEntry, hass: HomeAssistant) -> None:
        seen_during_cleanup.append(
            await _record(a.cluster.coordinator, f"entry.{DEV_ENTRY_ID}")
        )
        await original(self, hass)

    with patch.object(ConfigEntry, "async_remove", _remove):
        await a.hass.config_entries.async_remove(DEV_ENTRY_ID)

    assert seen_during_cleanup[0]["removing"] is True
    await wait_for(
        lambda: b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is None,
        timeout=10,
    )
    final = await _record(a.cluster.coordinator, f"entry.{DEV_ENTRY_ID}")
    assert final["removed"] is True
    inventory = await _record(a.cluster.coordinator, INVENTORY_KEY)
    assert DEV_ENTRY_ID not in inventory["entries"]
    assert DEV_ENTRY_ID in [item[0] for item in inventory["removed"]]
    # The standby only removed its copy: the clean-up ran once, on the leader.
    assert make_node.recorder.times("node_a", "cleanup")
    assert not make_node.recorder.times("node_b", "cleanup")


async def test_failed_clean_up_keeps_the_removal_announced(
    make_node: NodeFactory,
) -> None:
    """No tombstone is written over a clean-up that raised."""
    a = await make_node("node_a", start=False)
    install_custom(a.hass, "node_a", make_node.recorder, remove_raises=True)
    await start_node(a, make_node.url)

    await a.hass.config_entries.async_remove(DEV_ENTRY_ID)
    await a.cluster.storage.async_flush()

    record = await _record(a.cluster.coordinator, f"entry.{DEV_ENTRY_ID}")
    assert record["removing"] is True
    assert "removed" not in record
    assert make_node.recorder.times("node_a", "cleanup")


async def test_removal_refused_if_it_cannot_be_announced(
    make_node: NodeFactory,
) -> None:
    """Without the announcement the clean-up must not run."""
    a = await make_node("node_a")

    async def _fail(self: ClusterCoordinator, key: str, value: bytes) -> int:
        raise HomeAssistantError("NATS is away")

    with (
        patch.object(ClusterCoordinator, "async_fenced_put", _fail),
        pytest.raises(HomeAssistantError, match="Could not publish the removal"),
    ):
        await a.hass.config_entries.async_remove(DEV_ENTRY_ID)

    assert a.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is not None
    assert a.entry.state is LOADED
    assert not make_node.recorder.times("node_a", "cleanup")


async def test_announced_removal_is_completed_by_the_next_leader(
    make_node: NodeFactory, proxy: BlackholeProxy, hard_exit: HardExit
) -> None:
    """A leader that dies after announcing a removal: the next one finishes it.

    The next leader runs the clean-up itself (it may not have run) and
    publishes the tombstone; it never sets the entry up.
    """
    rec = make_node.recorder
    a = await make_node("node_a", proxy.url)
    b = await make_node("node_b")
    assert (a.entry.state, b.entry.state) == (LOADED, NOT_LOADED)

    with patch.object(LeaseManager, "async_restart"):
        await a.cluster.storage.async_publish_removing(a.entry)
        proxy.cut()  # the leader is gone before it could remove the entry

        await wait_for(
            lambda: b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is None,
            timeout=25,
        )
    assert rec.times("node_b", "cleanup")
    assert not rec.times("node_b", "setup")
    await wait_for(lambda: b.cluster.leases.is_leader, timeout=10)

    async def tombstone() -> bool:
        record = await _record(b.cluster.coordinator, f"entry.{DEV_ENTRY_ID}")
        return bool(record and record.get("removed"))

    for _ in range(50):
        if await tombstone():
            break
        await asyncio.sleep(0.2)
    else:
        pytest.fail("the next leader never published the tombstone")


async def test_removal_lost_with_the_bucket_is_applied_at_takeover(
    make_node: NodeFactory, proxy: BlackholeProxy
) -> None:
    """A standby that missed a removal, whose tombstone went with the bucket.

    The leader republishes its inventory (with its removal log) into the
    recreated bucket; the standby removes the entry when it takes over
    instead of running it.
    """
    rec = make_node.recorder
    a = await make_node("node_a")
    b = await make_node("node_b", proxy.url)
    await wait_for(lambda: DEV_ENTRY_ID in b.cluster.storage.memory.known, timeout=10)

    proxy.cut()
    await a.hass.config_entries.async_remove(DEV_ENTRY_ID)
    nc = await nats.connect(make_node.url)
    await nc.jetstream().delete_stream(f"KV_{BUCKET_STORAGE}")
    await nc.close()

    async def inventory_republished() -> bool:
        try:
            record = await _record(a.cluster.coordinator, INVENTORY_KEY)
        except Exception:  # noqa: BLE001 - the bucket is not back yet
            return False
        return bool(record) and DEV_ENTRY_ID in [i[0] for i in record["removed"]]

    for _ in range(100):
        if await inventory_republished():
            break
        await asyncio.sleep(0.2)
    else:
        pytest.fail("the leader never republished its inventory")

    proxy.restore()
    await asyncio.sleep(2)
    # The tombstone is gone: the standby still has the entry.
    assert b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is not None

    await a.hass.async_stop()
    await wait_for(lambda: b.cluster.leases.is_leader, timeout=20)
    await wait_for(
        lambda: b.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is None,
        timeout=10,
    )
    assert not rec.times("node_b", "setup")
    assert not rec.times("node_b", "cleanup")


async def test_missing_inventory_is_reported(make_node: NodeFactory) -> None:
    """A node that saw an inventory and finds none asks the user to check."""
    a = await make_node("node_a")
    storage = a.cluster.storage
    kv = a.cluster.coordinator.kv_storage
    await wait_for(lambda: storage.memory.seen_inventory, timeout=10)
    issues = a.issue_registry

    await kv.delete(INVENTORY_KEY)
    await storage._async_reconcile_inventory()
    assert issues.async_get_issue(ISSUE_DOMAIN, ISSUE_DATA_LOST) is not None
    assert a.hass.config_entries.async_get_entry(DEV_ENTRY_ID) is not None

    await storage.async_flush()  # the leader publishes it again
    await storage._async_reconcile_inventory()
    assert issues.async_get_issue(ISSUE_DOMAIN, ISSUE_DATA_LOST) is None


async def test_reauthentication_after_a_takeover_is_explained(
    make_node: NodeFactory,
) -> None:
    """A reauth flow soon after a takeover points at the lost token."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await a.hass.async_stop()
    await wait_for(lambda: b.entry.state is LOADED, timeout=15)

    b.entry.async_start_reauth(b.hass)
    await b.hass.async_block_till_done()

    issue = b.issue_registry.async_get_issue(
        ISSUE_DOMAIN, ISSUE_TOKEN_LOST.format(entry_id=DEV_ENTRY_ID)
    )
    assert issue is not None
    assert issue.translation_placeholders == {"title": "Dev", "previous": "node_a"}


async def test_takeover_without_the_application_credential_is_reported(
    make_node: NodeFactory,
) -> None:
    """An OAuth entry whose credential this node lacks gets a repair issue."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    a.hass.config_entries.async_update_entry(
        a.entry, data={**a.entry.data, "auth_implementation": "dev_credential"}
    )
    await wait_for(
        lambda: b.entry.data.get("auth_implementation") == "dev_credential",
        timeout=10,
    )

    await a.hass.async_stop()
    await wait_for(lambda: b.entry.state is LOADED, timeout=15)

    issue = b.issue_registry.async_get_issue(
        ISSUE_DOMAIN, ISSUE_MISSING_CREDENTIAL.format(entry_id=DEV_ENTRY_ID)
    )
    assert issue is not None
    assert issue.translation_placeholders["credential"] == "dev_credential"


async def test_republish_after_bucket_loss_includes_unchanged_stores(
    make_node: NodeFactory,
) -> None:
    """A store that did not change since the loss is published from its file."""
    a = await make_node("node_a")
    stored = json.dumps({"version": 1, "key": "core.area_registry", "data": {}})
    path = a.hass.config.path(".storage", "core.area_registry")
    await a.hass.async_add_executor_job(lambda: Path(path).parent.mkdir(exist_ok=True))
    await a.hass.async_add_executor_job(Path(path).write_text, stored)

    try:
        await a.cluster.storage.async_republish()
        record = await a.cluster.coordinator.kv_storage.get("store.core.area_registry")
    finally:
        await a.hass.async_add_executor_job(Path(path).unlink)

    assert json.loads(record.value)["content"] == json.loads(stored)


async def test_unfinished_removal_is_announced_again_after_bucket_loss(
    make_node: NodeFactory,
) -> None:
    """A removal whose clean-up failed survives the loss of the storage bucket."""
    a = await make_node("node_a", start=False)
    install_custom(a.hass, "node_a", make_node.recorder, remove_raises=True)
    await start_node(a, make_node.url)
    await a.hass.config_entries.async_remove(DEV_ENTRY_ID)
    await a.cluster.storage.async_flush()
    assert a.cluster.storage.memory.intents

    nc = await nats.connect(make_node.url)
    await nc.jetstream().delete_stream(f"KV_{BUCKET_STORAGE}")
    await nc.close()
    await a.cluster.coordinator.async_ensure_buckets()
    await a.cluster.coordinator.async_start_term("node_a")
    await a.cluster.storage.async_republish()

    record = await _record(a.cluster.coordinator, f"entry.{DEV_ENTRY_ID}")
    assert record["removing"] is True
    assert record["entry"]["entry_id"] == DEV_ENTRY_ID


async def test_removal_announced_again_after_bucket_loss_is_sealed(
    make_node: NodeFactory,
) -> None:
    """The republished announcement carries the whole entry: it is encrypted too.

    Fails with the ``_seal`` call of the republish path removed: the raw record
    then holds the token in plain text.
    """
    secret = "republished-secret-token-xyz"
    a = await make_node("node_a", start=False)
    install_custom(a.hass, "node_a", make_node.recorder, remove_raises=True)
    await start_node(a, make_node.url)
    a.hass.config_entries.async_update_entry(a.entry, data={"token": secret})
    await a.hass.config_entries.async_remove(DEV_ENTRY_ID)
    await a.cluster.storage.async_flush()
    assert a.cluster.storage.memory.intents

    nc = await nats.connect(make_node.url)
    await nc.jetstream().delete_stream(f"KV_{BUCKET_STORAGE}")
    await nc.close()
    await a.cluster.coordinator.async_ensure_buckets()
    await a.cluster.coordinator.async_start_term("node_a")
    await a.cluster.storage.async_republish()

    key = f"entry.{DEV_ENTRY_ID}"
    raw = (await a.cluster.coordinator.kv_storage.get(key)).value
    assert secret.encode() not in raw
    stored = json.loads(raw)
    assert "entry" not in stored
    assert "enc" in stored
    assert stored["removing"] is True  # what routes a record stays readable
    # A node with the key reads it.
    assert open_entry_payload(key, raw)["entry"]["data"] == {"token": secret}


async def test_provenance_of_a_known_entry_is_saved_at_once(
    make_node: NodeFactory,
) -> None:
    """An entry the cluster published is remembered before any delay."""
    a = await make_node("node_a")
    memory = a.cluster.storage.memory

    with patch.object(memory._store, "async_save", AsyncMock()) as save:
        await memory.async_mark_known("01NEWENTRY")

    save.assert_awaited_once()
    assert "01NEWENTRY" in save.await_args.args[0]["known"]


def test_repair_issue_texts_exist_in_the_homeassistant_translations() -> None:
    """The cluster has no strings of its own; its issues use these keys."""
    strings = json.loads(
        (
            Path(__file__).parents[2]
            / "homeassistant/components/homeassistant/strings.json"
        ).read_text()
    )["issues"]

    for key in (
        "cluster_storage_data_lost",
        "cluster_missing_credential",
        "cluster_token_lost",
    ):
        assert strings[key]["title"]
        assert strings[key]["description"]
