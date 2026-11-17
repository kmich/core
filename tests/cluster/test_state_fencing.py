"""States of a superseded leader are not mirrored, and ghosts do not stay.

Tests against a real NATS JetStream server.
"""

from nats.js.errors import KeyDeletedError, KeyNotFoundError
import pytest

from homeassistant.config_entries import ConfigEntryDisabler
from homeassistant.core import State
from homeassistant.helpers.json import json_bytes

from .conftest import DEV_ENTRY_ID, NodeFactory
from .test_init import wait_for

DEV_SWITCH = "switch.dev_switch"


def _payload(term: int | None, value: str) -> bytes:
    data = {
        "origin": "node_a",
        "config_entry_id": DEV_ENTRY_ID,
        "integration": "cluster_dev",
        "state": State("switch.ghost", value).as_dict(),
    }
    if term is not None:
        data["term"] = term
    return json_bytes(data)


async def test_state_of_a_superseded_term_is_not_mirrored(
    make_node: NodeFactory,
) -> None:
    """A delayed write of a leader that was superseded must not show up."""
    await make_node("node_a")
    b = await make_node("node_b")
    replicator = b.cluster.replicator
    b.cluster.coordinator.note_term(5)

    replicator._async_apply("switch.ghost", "PUT", _payload(4, "on"))
    assert b.hass.states.get("switch.ghost") is None

    replicator._async_apply("switch.ghost", "PUT", _payload(5, "on"))
    assert b.hass.states.get("switch.ghost").state == "on"

    replicator._async_apply("switch.ghost", "PUT", _payload(3, "off"))
    assert b.hass.states.get("switch.ghost").state == "on"

    # A node that does not write a term (an older one) is not stale.
    replicator._async_apply("switch.ghost", "PUT", _payload(None, "off"))
    assert b.hass.states.get("switch.ghost").state == "off"


async def test_new_term_starts_above_every_term_seen(make_node: NodeFactory) -> None:
    """After a bucket loss the stored term is gone; the counter must not restart."""
    a = await make_node("node_a")
    coordinator = a.cluster.coordinator
    coordinator.note_term(10)

    await coordinator.async_start_term("node_a")

    assert coordinator.term == 11
    assert coordinator.observed_term == 11


async def test_delete_does_not_remove_a_newer_leaders_state(
    make_node: NodeFactory,
) -> None:
    """A delete is conditional on the revision of the node's own last write."""
    a = await make_node("node_a")
    replicator = a.cluster.replicator
    kv = a.cluster.coordinator.kv_states
    key = "switch.contested"
    replicator._revisions[key] = await kv.put(key, b"old leader")
    await kv.put(key, b"new leader")

    await replicator._async_delete(kv, key)

    assert (await kv.get(key)).value == b"new leader"

    replicator._revisions[key] = (await kv.get(key)).revision
    await replicator._async_delete(kv, key)
    with pytest.raises((KeyNotFoundError, KeyDeletedError)):
        await kv.get(key)


async def test_disabling_an_entry_removes_its_mirrored_states(
    make_node: NodeFactory,
) -> None:
    """No ghost state stays on a standby after the leader disabled the entry."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: b.hass.states.get(DEV_SWITCH) is not None, timeout=10)

    await a.hass.config_entries.async_set_disabled_by(
        a.entry.entry_id, ConfigEntryDisabler.USER
    )

    await wait_for(lambda: b.hass.states.get(DEV_SWITCH) is None, timeout=10)
    assert DEV_SWITCH not in b.cluster.replicator.mirrored


async def test_removing_an_entry_removes_its_mirrored_states(
    make_node: NodeFactory,
) -> None:
    """No ghost state stays on a standby after the leader removed the entry."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: b.hass.states.get(DEV_SWITCH) is not None, timeout=10)

    await a.hass.config_entries.async_remove(a.entry.entry_id)

    await wait_for(lambda: b.hass.states.get(DEV_SWITCH) is None, timeout=10)
    assert DEV_SWITCH not in b.cluster.replicator.mirrored


async def test_unchanged_states_are_published_again_after_a_term_restart(
    make_node: NodeFactory,
) -> None:
    """A standby that replays the bucket drops states of an older term.

    After the term key alone was lost and the leader started a term again, the
    leader's unchanged states must carry the new term, or a standby that has
    seen it (and replays) would lose them.
    """
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(lambda: b.hass.states.get(DEV_SWITCH) is not None, timeout=10)
    coordinator = a.cluster.coordinator
    first_term = coordinator.term

    # The term key is lost; the leader notices when a write is refused.
    await coordinator.kv_storage.purge("cluster.term")
    coordinator.fence_revision = None
    await a.cluster._async_check_buckets()
    assert coordinator.term > first_term

    # The standby sees the new term (through any record), and replays.
    b.cluster.coordinator.note_term(coordinator.term)
    b.cluster.replicator.mirrored.pop(DEV_SWITCH, None)
    b.hass.states.async_remove(DEV_SWITCH)
    b.cluster.replicator._start_watcher()

    await wait_for(lambda: b.hass.states.get(DEV_SWITCH) is not None, timeout=10)
