"""The cluster with one NATS user per node, in strict mode (opt-in).

Needs the server of ``harness/nats-permissions.conf``: set
``CLUSTER_TEST_NATS_URL`` to it with the admin user (for the clean-up), for
example ``nats://admin:secret_admin@127.0.0.1:4222``, and
``CLUSTER_TEST_NATS_PERMISSIONS=1``. The nodes connect as ``node_a`` and
``node_b`` with the permissions of that file, so a subject or KV key that
the cluster uses and the file does not allow fails these tests.
"""

import logging
import os
from urllib.parse import urlsplit

import pytest

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import Event

from .conftest import DEV_ENTITY, TEST_ENCRYPTION_KEY, NodeFactory
from .test_init import wait_for

pytestmark = pytest.mark.skipif(
    not os.environ.get("CLUSTER_TEST_NATS_PERMISSIONS"),
    reason="set CLUSTER_TEST_NATS_PERMISSIONS=1 with a server of nats-permissions.conf",
)

LOADED = ConfigEntryState.LOADED
NOT_LOADED = ConfigEntryState.NOT_LOADED


def _node_conf(node: str) -> dict[str, str]:
    return {
        "nats_user": node,
        "nats_password": f"secret_{node[-1]}",
        "security": "strict",
        "encryption_key": TEST_ENCRYPTION_KEY,
    }


async def test_cluster_runs_with_one_user_per_node(
    make_node: NodeFactory, caplog: pytest.LogCaptureFixture
) -> None:
    """Election, forwarded calls, events and a failover, all within the permissions."""
    caplog.set_level(logging.WARNING)
    url = urlsplit(make_node.url)
    plain = f"{url.scheme}://{url.hostname}:{url.port}"
    a = await make_node("node_a", plain, **_node_conf("node_a"))
    b = await make_node("node_b", plain, **_node_conf("node_b"))
    assert (a.entry.state, b.entry.state) == (LOADED, NOT_LOADED)
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY) is not None, timeout=10)

    # A call on the standby runs on the leader (RPC and its reply).
    await b.hass.services.async_call(
        "switch", "turn_on", {"entity_id": DEV_ENTITY}, blocking=True
    )
    await wait_for(lambda: a.hass.states.get(DEV_ENTITY).state == "on", timeout=5)
    await wait_for(lambda: b.hass.states.get(DEV_ENTITY).state == "on", timeout=5)

    # An event fired on the standby reaches the leader.
    seen: list[Event] = []
    a.hass.bus.async_listen("cluster_test_event", seen.append)
    b.hass.bus.async_fire("cluster_test_event", {"n": 1})
    await wait_for(lambda: len(seen) == 1, timeout=5)

    # The leader stops; the standby takes over (term, records, inventory).
    await a.hass.async_stop()
    await wait_for(lambda: b.entry.state is LOADED, timeout=15)

    assert "permissions violation" not in caplog.text.lower()
