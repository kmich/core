"""Peer identity, roles and the security setting (section 1 of the review items).

The sender of an RPC call or an event is the last token of its subject,
which NATS permissions restrict to that node's user (the README has the
server configuration). These tests run against an unauthenticated test
server, so they check what the receiver trusts, not the permissions.
"""

import asyncio
from typing import Any

import nats
import nats.errors
import probatio
import pytest

from homeassistant.auth.const import GROUP_ID_ADMIN
from homeassistant.cluster import CONFIG_SCHEMA, async_setup_cluster
from homeassistant.cluster.const import DOMAIN, MAX_INBOUND_RPC
from homeassistant.core import Event, HomeAssistant, ServiceCall
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.json import json_bytes
from homeassistant.util.json import json_loads_object

from .conftest import TEST_ENCRYPTION_KEY, NodeFactory
from .test_init import wait_for


def _conf(**extra: Any) -> dict[str, Any]:
    return {
        DOMAIN: {"node_id": "node_a", "encryption_key": TEST_ENCRYPTION_KEY, **extra}
    }


@pytest.mark.parametrize(
    ("conf", "error"),
    [
        pytest.param(_conf(), "configure NATS credentials", id="no_credentials"),
        pytest.param(
            {DOMAIN: {"node_id": "node_a", "nats_user": "a", "nats_password": "p"}},
            "configure encryption_key",
            id="no_encryption_key",
        ),
        pytest.param(
            _conf(nats_user="a", nats_password="p", encryption_key="too-short"),
            "length of value must be at least",
            id="short_encryption_key",
        ),
        pytest.param(
            _conf(nats_url="nats://nats:4222", nats_user="a", nats_password="p"),
            "is not encrypted",
            id="plain_remote_url",
        ),
        pytest.param(
            _conf(
                nats_url="tls://nats:4222,nats://other:4222",
                nats_user="a",
                nats_password="p",
            ),
            "the connection to other:4222 is not encrypted",
            id="one_plain_url",
        ),
        pytest.param(
            _conf(nats_url="tls://nats:4222", nats_token="t"),
            "does not tell the nodes apart",
            id="shared_token",
        ),
        pytest.param(
            _conf(nats_url="tls://nats:4222", nats_user="a", nats_password="p"),
            None,
            id="tls",
        ),
        pytest.param(
            _conf(nats_url="nats://127.0.0.1:4222", nats_user="a", nats_password="p"),
            None,
            id="loopback",
        ),
        pytest.param(
            _conf(nats_url="nats://nats:4222", security="permissive"),
            None,
            id="permissive",
        ),
    ],
)
def test_strict_security_by_default(conf: dict[str, Any], error: str | None) -> None:
    """Strict mode needs credentials, and TLS off this host."""
    if error is None:
        CONFIG_SCHEMA(conf)
        return
    with pytest.raises(probatio.Invalid, match=error):
        CONFIG_SCHEMA(conf)


async def test_strict_mode_error_never_quotes_the_password_of_the_url(
    hass: HomeAssistant,
    issue_registry: ir.IssueRegistry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The log line and the repair issue of a refused configuration are free of secrets."""
    conf = _conf(
        nats_url="nats://someone:s3cret_pw@nats.example:4222",
        nats_user="a",
        nats_password="s3cret_other",
    )[DOMAIN]

    assert not await async_setup_cluster(hass, conf)

    issue = issue_registry.async_get_issue("homeassistant", "cluster_setup_failed")
    assert issue is not None
    reason = issue.translation_placeholders["reason"]
    assert "nats.example" in reason
    assert "not encrypted" in reason
    for text in (caplog.text, reason):
        assert "s3cret_pw" not in text
        assert "someone" not in text
        assert "s3cret_other" not in text


async def test_setup_failure_of_a_check_of_the_whole_section_names_what_to_fix(
    hass: HomeAssistant, issue_registry: ir.IssueRegistry
) -> None:
    """Strict mode without an encryption key: the issue says which setting is missing."""
    conf = _conf(nats_user="a", nats_password="p")[DOMAIN]
    del conf["encryption_key"]

    assert not await async_setup_cluster(hass, conf)

    issue = issue_registry.async_get_issue("homeassistant", "cluster_setup_failed")
    assert issue is not None
    assert "encryption_key" in issue.translation_placeholders["reason"]


async def _register_recorder(hass: HomeAssistant) -> list[ServiceCall]:
    calls: list[ServiceCall] = []

    async def _record(call: ServiceCall) -> None:
        calls.append(call)

    hass.services.async_register("cluster_test", "record", _record)
    return calls


async def _rpc(url: str, subject: str, body: dict[str, Any]) -> dict[str, Any]:
    nc = await nats.connect(url)
    try:
        request = {
            "domain": "cluster_test",
            "service": "record",
            "service_data": {},
            "blocking": True,
            "return_response": False,
            "context": {"id": None, "parent_id": None, "user_id": "spoofed"},
            **body,
        }
        reply = await nc.request(subject, json_bytes(request), timeout=5)
        return json_loads_object(reply.data)
    finally:
        await nc.close()


async def test_rpc_sender_comes_from_the_subject(make_node: NodeFactory) -> None:
    """A body that names another sender than the subject is refused."""
    a = await make_node("node_a")
    calls = await _register_recorder(a.hass)

    reply = await _rpc(
        make_node.url,
        "hass.rpc.node_a.from.node_b",
        {"origin": "node_c", "role": "admin"},
    )
    assert reply["ok"] is False
    assert "claims to come from node_c" in reply["error"]["message"]

    with pytest.raises(nats.errors.NoRespondersError):  # the old subject
        await _rpc(make_node.url, "hass.rpc.node_a.call", {"role": "admin"})
    assert calls == []


async def _peer(make_node: NodeFactory) -> dict[str, Any]:
    """Start node_a and node_b; the body fields that identify node_b to node_a."""
    a = await make_node("node_a")
    b = await make_node("node_b")
    await wait_for(
        lambda: a.cluster.leases.peer_incarnation("node_b") is not None, timeout=10
    )
    return {"origin": "node_b", "instance": b.cluster.leases.incarnation}


async def test_rpc_needs_the_identity_of_a_member(make_node: NodeFactory) -> None:
    """A body without the sender, or with a run that is not a member, is refused."""
    identity = await _peer(make_node)
    calls = await _register_recorder(make_node.nodes[0].hass)
    subject = "hass.rpc.node_a.from.node_b"

    for body in (
        {"role": "admin"},
        {"role": "admin", "origin": "node_b"},
        {"role": "admin", "origin": "node_b", "instance": "somebody-else"},
        {"role": "admin", "instance": identity["instance"]},
    ):
        reply = await _rpc(make_node.url, subject, body)
        assert reply["ok"] is False, body
    assert calls == []

    assert (await _rpc(make_node.url, subject, {"role": "admin", **identity}))["ok"]


async def test_rpc_runs_as_the_role_user(make_node: NodeFactory) -> None:
    """A forwarded call runs as the system user of its role; a user id is ignored."""
    identity = await _peer(make_node)
    a = make_node.nodes[0]
    calls = await _register_recorder(a.hass)

    reply = await _rpc(
        make_node.url, "hass.rpc.node_a.from.node_b", {"role": "admin", **identity}
    )
    assert reply["ok"] is True, reply
    reply = await _rpc(
        make_node.url, "hass.rpc.node_a.from.node_b", {"role": "system", **identity}
    )
    assert reply["ok"] is True, reply

    role_user = await a.hass.auth.async_get_user(calls[0].context.user_id)
    assert role_user is not None
    assert role_user.system_generated
    assert [g.id for g in role_user.groups] == [GROUP_ID_ADMIN]
    assert calls[1].context.user_id is None


async def test_rpc_beyond_the_limit_is_refused_at_once(make_node: NodeFactory) -> None:
    """A flood of calls is answered "busy" instead of piling up tasks."""
    a = await make_node("node_a")
    calls = await _register_recorder(a.hass)
    a.cluster.router._inbound = MAX_INBOUND_RPC

    # Refused before the call is looked at: no identity is needed.
    reply = await _rpc(make_node.url, "hass.rpc.node_a.from.node_b", {"role": "admin"})
    assert reply == {
        "ok": False,
        "error": {"type": "HomeAssistantError", "message": "Node node_a is busy"},
    }
    assert calls == []


async def test_event_sender_comes_from_the_subject(make_node: NodeFactory) -> None:
    """An event is fired only under the subject's sender, and without a user."""
    a = await make_node("node_a")
    fired: list[Event] = []
    a.hass.bus.async_listen("cluster_test_event", fired.append)
    nc = await nats.connect(make_node.url)

    def payload(origin: str) -> bytes:
        return json_bytes(
            {
                "origin": origin,
                "event_type": "cluster_test_event",
                "data": {"from": origin},
                "context": {"id": None, "parent_id": None, "user_id": "spoofed"},
            }
        )

    await nc.publish("hass.event.node_b", payload("node_c"))  # claims another
    await nc.publish("hass.event.node_b", payload("node_b"))
    await nc.flush()
    await nc.close()

    await wait_for(lambda: len(fired) == 1, timeout=5)
    await asyncio.sleep(0.3)
    assert len(fired) == 1
    assert fired[0].data == {"from": "node_b"}
    assert fired[0].context.user_id is None
