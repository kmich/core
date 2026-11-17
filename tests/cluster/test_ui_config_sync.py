"""UI automations, scripts, scenes and helpers follow the leader.

Every node gets its own configuration directory. In the other cluster tests
all nodes share ``tests/testing_config``, where a standby that writes
``automations.yaml`` would write the leader's own file, and a test would pass
without any sync. The leader's changes go through the real config editor
HTTP API and the real collection websocket API.
"""

import asyncio
from collections.abc import Awaitable, Callable
from http import HTTPStatus
from pathlib import Path
import time
from typing import Any
from unittest.mock import PropertyMock, patch

from aiohttp.test_utils import TestClient
import nats.errors
from nats.js.errors import KeyNotFoundError
import pytest
from pytest_homeassistant.typing import ClientSessionGenerator, WebSocketGenerator

from homeassistant import config as conf_util
from homeassistant.cluster.coordinator import ClusterCoordinator
from homeassistant.components import config
from homeassistant.components.config import (
    automation as config_automation,
    scene as config_scene,
    script as config_script,
)
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import cluster as cluster_helper
from homeassistant.helpers.json import json_bytes
from homeassistant.setup import async_setup_component
from homeassistant.util.json import json_loads_object
from homeassistant.util.yaml import load_yaml

from .conftest import Node, NodeFactory, start_node
from .test_init import record_calls, wait_for
from .test_readiness import fail_over_gracefully

LOADED = ConfigEntryState.LOADED

CONFIGURATION = (
    "automation: !include automations.yaml\n"
    "script: !include scripts.yaml\n"
    "scene: !include scenes.yaml\n"
)
UI_FILES = {"automations.yaml": "[]\n", "scripts.yaml": "{}\n", "scenes.yaml": "[]\n"}
UI_DOMAINS = ("automation", "script", "scene", "input_boolean", "counter")

AUTOMATION_ID = "d3_ping_id"
AUTOMATION = {
    "alias": "Ping",
    "triggers": [{"trigger": "event", "event_type": "d3_ping"}],
    "actions": [{"action": "test.record"}],
}
PING = "automation.ping"
SCRIPT = {"alias": "Script", "sequence": [{"action": "test.record"}]}
SCENE_ID = "d3_scene_id"
SCENE = {"name": "Scene", "entities": {"input_boolean.nothing": "on"}}
GUEST = "input_boolean.guest_mode"
VISITS = "counter.visits"


def _write_config_dir(path: Path) -> None:
    path.mkdir()
    (path / "configuration.yaml").write_text(CONFIGURATION)
    for name, content in UI_FILES.items():
        (path / name).write_text(content)


async def make_ui_node(make_node: NodeFactory, root: Path, name: str) -> Node:
    """Start a node with its own configuration directory and the UI domains."""
    node = await make_node(name, start=False)
    config_dir = root / name
    await node.hass.async_add_executor_job(_write_config_dir, config_dir)
    node.hass.config.config_dir = str(config_dir)
    # Read as bootstrap reads it: before the cluster starts.
    conf = await conf_util.async_hass_config_yaml(node.hass)
    await start_node(node, make_node.url)
    for domain in UI_DOMAINS:
        assert await async_setup_component(node.hass, domain, conf)
    await node.hass.async_block_till_done()
    return node


async def open_editor(
    hass: HomeAssistant, hass_client: ClientSessionGenerator
) -> TestClient:
    """Set up the config editor for automations, scripts and scenes."""
    with patch.object(
        config, "SECTIONS", [config_automation, config_script, config_scene]
    ):
        assert await async_setup_component(hass, "config", {})
    return await hass_client()


async def read_file(node: Node, name: str) -> Any:
    """Parse one of the UI files in the node's own configuration directory."""
    return await node.hass.async_add_executor_job(
        load_yaml, node.hass.config.path(name)
    )


async def create_helper(
    hass: HomeAssistant, hass_ws_client: WebSocketGenerator, domain: str, name: str
) -> str:
    """Create a helper through the websocket API; return its id."""
    ws = await hass_ws_client(hass)
    await ws.send_json_auto_id({"type": f"{domain}/create", "name": name})
    msg = await ws.receive_json()
    assert msg["success"], msg
    return msg["result"]["id"]


async def test_automation_made_in_the_leaders_ui_runs_after_failover(
    make_node: NodeFactory, tmp_path: Path, hass_client: ClientSessionGenerator
) -> None:
    """An automation saved in the leader's editor runs on the next leader."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")
    assert a.hass.config.path("automations.yaml") != b.hass.config.path(
        "automations.yaml"
    )
    calls = {node.name: record_calls(node.hass) for node in (a, b)}
    client = await open_editor(a.hass, hass_client)

    resp = await client.post(
        f"/api/config/automation/config/{AUTOMATION_ID}", json=AUTOMATION
    )
    assert resp.status == HTTPStatus.OK
    await wait_for(lambda: a.hass.states.get(PING) is not None)

    # The standby has it live, from its own file.
    await wait_for(lambda: b.hass.states.get(PING) is not None, timeout=10)
    assert [item["id"] for item in await read_file(b, "automations.yaml")] == [
        AUTOMATION_ID
    ]

    await fail_over_gracefully(a.hass)
    await wait_for(lambda: cluster_helper.async_is_leader(b.hass), timeout=10)
    await b.hass.async_block_till_done()

    b.hass.bus.async_fire("d3_ping")
    await wait_for(lambda: len(calls["node_b"]) == 1)
    await asyncio.sleep(0.5)
    assert len(calls["node_b"]) == 1
    assert calls["node_a"] == []


async def test_script_and_scene_made_on_the_leader_reach_the_standby(
    make_node: NodeFactory, tmp_path: Path, hass_client: ClientSessionGenerator
) -> None:
    """Scripts and scenes saved in the leader's editor exist on a standby."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")
    client = await open_editor(a.hass, hass_client)

    resp = await client.post("/api/config/script/config/d3_script", json=SCRIPT)
    assert resp.status == HTTPStatus.OK
    resp = await client.post(f"/api/config/scene/config/{SCENE_ID}", json=SCENE)
    assert resp.status == HTTPStatus.OK

    await wait_for(
        lambda: (
            b.hass.states.get("script.d3_script") is not None
            and b.hass.states.get("scene.scene") is not None
        ),
        timeout=10,
    )
    assert list(await read_file(b, "scripts.yaml")) == ["d3_script"]
    assert [item["id"] for item in await read_file(b, "scenes.yaml")] == [SCENE_ID]


async def test_automation_deleted_on_the_leader_is_deleted_on_the_standby(
    make_node: NodeFactory, tmp_path: Path, hass_client: ClientSessionGenerator
) -> None:
    """A delete in the editor removes the automation and its registry entry."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")
    client = await open_editor(a.hass, hass_client)
    resp = await client.post(
        f"/api/config/automation/config/{AUTOMATION_ID}", json=AUTOMATION
    )
    assert resp.status == HTTPStatus.OK
    await wait_for(lambda: b.hass.states.get(PING) is not None, timeout=10)

    resp = await client.delete(f"/api/config/automation/config/{AUTOMATION_ID}")
    assert resp.status == HTTPStatus.OK

    await wait_for(lambda: b.hass.states.get(PING) is None, timeout=10)
    assert (
        b.entity_registry.async_get_entity_id("automation", "automation", AUTOMATION_ID)
        is None
    )
    assert await read_file(b, "automations.yaml") == []


async def test_helper_made_on_the_leader_follows_live(
    make_node: NodeFactory, tmp_path: Path, hass_ws_client: WebSocketGenerator
) -> None:
    """A helper created, changed and deleted on the leader follows on a standby."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")

    item_id = await create_helper(a.hass, hass_ws_client, "input_boolean", "Guest mode")
    await wait_for(lambda: b.hass.states.get(GUEST) is not None, timeout=10)
    assert (
        b.entity_registry.async_get_entity_id("input_boolean", "input_boolean", item_id)
        == GUEST
    )

    ws = await hass_ws_client(a.hass)
    await ws.send_json_auto_id(
        {
            "type": "input_boolean/update",
            "input_boolean_id": item_id,
            "name": "Guest mode",
            "icon": "mdi:account-group",
        }
    )
    assert (await ws.receive_json())["success"]
    await wait_for(
        lambda: (
            (state := b.hass.states.get(GUEST)) is not None
            and state.attributes.get("icon") == "mdi:account-group"
        ),
        timeout=10,
    )

    await ws.send_json_auto_id(
        {"type": "input_boolean/delete", "input_boolean_id": item_id}
    )
    assert (await ws.receive_json())["success"]
    await wait_for(lambda: b.hass.states.get(GUEST) is None, timeout=10)


async def _make_helpers_with_state(
    a: Node, b: Node, hass_ws_client: WebSocketGenerator
) -> None:
    """On the leader: a guest mode switch that is on, a counter at 2."""
    await create_helper(a.hass, hass_ws_client, "input_boolean", "Guest mode")
    await create_helper(a.hass, hass_ws_client, "counter", "Visits")
    await a.hass.services.async_call(
        "input_boolean", "turn_on", {"entity_id": GUEST}, blocking=True
    )
    for _ in range(2):
        await a.hass.services.async_call(
            "counter", "increment", {"entity_id": VISITS}, blocking=True
        )
    assert a.hass.states.get(GUEST).state == "on"
    assert a.hass.states.get(VISITS).state == "2"
    await wait_for(
        lambda: (
            b.hass.states.get(GUEST) is not None
            and b.hass.states.get(VISITS) is not None
        ),
        timeout=10,
    )


async def test_helper_state_follows_a_graceful_failover(
    make_node: NodeFactory, tmp_path: Path, hass_ws_client: WebSocketGenerator
) -> None:
    """The next leader's helpers have the values they had on the old leader."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")
    await _make_helpers_with_state(a, b, hass_ws_client)

    await fail_over_gracefully(a.hass)
    await wait_for(lambda: b.entry.state is LOADED, timeout=10)

    assert b.hass.states.get(GUEST).state == "on"
    assert b.hass.states.get(VISITS).state == "2"


async def test_helper_state_follows_a_crash(
    make_node: NodeFactory, tmp_path: Path, hass_ws_client: WebSocketGenerator
) -> None:
    """Helper values survive a leader that dies without a shutdown."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")
    await _make_helpers_with_state(a, b, hass_ws_client)
    await asyncio.sleep(1)

    # A crash: no final save, no flush, no release.
    await a.cluster.leases.async_stop(release=False)
    await wait_for(lambda: b.entry.state is LOADED, timeout=15)

    assert b.hass.states.get(GUEST).state == "on"
    assert b.hass.states.get(VISITS).state == "2"


async def test_standby_that_starts_late_gets_the_ui_configuration(
    make_node: NodeFactory,
    tmp_path: Path,
    hass_client: ClientSessionGenerator,
    hass_ws_client: WebSocketGenerator,
) -> None:
    """A node that joins later has what was made in the leader's UI before."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    client = await open_editor(a.hass, hass_client)
    resp = await client.post(
        f"/api/config/automation/config/{AUTOMATION_ID}", json=AUTOMATION
    )
    assert resp.status == HTTPStatus.OK
    await create_helper(a.hass, hass_ws_client, "input_boolean", "Guest mode")
    await wait_for(lambda: a.hass.states.get(PING) is not None)
    await asyncio.sleep(2)

    b = await make_ui_node(make_node, tmp_path, "node_b")

    await wait_for(
        lambda: (
            b.hass.states.get(PING) is not None and b.hass.states.get(GUEST) is not None
        ),
        timeout=10,
    )


def failing_ui_put(calls: list[str]) -> Callable[..., Awaitable[int]]:
    """A fenced put that times out for the UI configuration records."""
    real_put = ClusterCoordinator.async_fenced_put

    async def put(self: ClusterCoordinator, key: str, value: bytes) -> int:
        if not key.startswith(("ui.", "ui_state.")):
            return await real_put(self, key, value)
        calls.append(key)
        await asyncio.sleep(0.01)
        raise nats.errors.TimeoutError

    return put


async def put_record(node: Node, key: str, payload: dict[str, Any]) -> None:
    """Publish a record as another node would."""
    kv = node.cluster.coordinator.kv_storage
    assert kv is not None
    await kv.put(key, json_bytes({"origin": "node_x", "ts": time.time(), **payload}))


async def read_text(node: Node, name: str) -> str:
    """The text of a file in the node's own configuration directory."""
    return await node.hass.async_add_executor_job(
        Path(node.hass.config.path(name)).read_text
    )


async def test_leader_does_not_apply_ui_records(
    make_node: NodeFactory, tmp_path: Path
) -> None:
    """The node that holds the lease keeps its own files."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    await asyncio.sleep(1.5)

    await put_record(
        a,
        "ui.file.automations.yaml",
        {"domain": "automation", "content": "- id: other\n"},
    )
    await asyncio.sleep(1.5)

    assert await read_file(a, "automations.yaml") == []


async def test_only_the_editor_files_are_written(
    make_node: NodeFactory, tmp_path: Path
) -> None:
    """A record cannot make a standby write any other file."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")

    await put_record(
        a,
        "ui.file.configuration.yaml",
        {"domain": "automation", "content": "broken: [\n"},
    )
    await put_record(
        a,
        "ui.file.scripts.yaml",
        {"domain": "script", "content": "d3_marker:\n  sequence: []\n"},
    )

    await wait_for(lambda: b.hass.states.get("script.d3_marker") is not None)
    assert await read_text(b, "configuration.yaml") == CONFIGURATION


async def test_helpers_of_another_storage_version_are_not_applied(
    make_node: NodeFactory, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A standby does not load helpers stored in a format it does not know."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")

    await put_record(
        a,
        "ui.collection.input_boolean",
        {
            "version": 99,
            "minor_version": 1,
            "data": {"items": [{"id": "future", "name": "Future"}]},
        },
    )
    await wait_for(lambda: "storage version is 99.1" in caplog.text)

    assert b.hass.states.get("input_boolean.future") is None


async def test_failed_ui_publication_makes_the_flush_fail(
    make_node: NodeFactory, tmp_path: Path, hass_ws_client: WebSocketGenerator
) -> None:
    """The shutdown flush does not succeed over an unpublished helper."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    storage = a.cluster.storage
    kv = a.cluster.coordinator.kv_storage
    assert kv is not None
    calls: list[str] = []
    # Nothing else is waiting: the helpers' integrations were just loaded.
    await asyncio.wait_for(storage.async_flush(), 15)

    with patch.object(ClusterCoordinator, "async_fenced_put", failing_ui_put(calls)):
        await create_helper(a.hass, hass_ws_client, "input_boolean", "Guest mode")
        await wait_for(lambda: "ui.collection.input_boolean" in calls)
        with pytest.raises(HomeAssistantError):
            await asyncio.wait_for(storage.async_flush(), 15)

    await asyncio.wait_for(storage.async_flush(), 15)
    record = json_loads_object((await kv.get("ui.collection.input_boolean")).value)
    assert [item["id"] for item in record["data"]["items"]] == ["guest_mode"]


async def test_ui_file_larger_than_max_payload_is_not_published(
    make_node: NodeFactory, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A file the server cannot take is skipped with a warning.

    The record of its earlier, smaller version is deleted: another node must
    keep its own copy rather than take the old one for the current one.
    """
    a = await make_ui_node(make_node, tmp_path, "node_a")
    kv = a.cluster.coordinator.kv_storage
    assert kv is not None
    await asyncio.sleep(1.5)
    before = (await kv.get("ui.file.scenes.yaml")).revision

    with patch.object(
        ClusterCoordinator, "max_payload", new_callable=PropertyMock, return_value=500
    ):
        content = "".join(
            f"- id: scene_{i}\n  name: Scene {i}\n  entities: {{}}\n" for i in range(40)
        )
        await a.hass.async_add_executor_job(
            Path(a.hass.config.path("scenes.yaml")).write_text, content
        )
        await wait_for(
            lambda: (
                "ui.file.scenes.yaml (" in caplog.text
                and "exceeds the NATS max_payload" in caplog.text
            )
        )

        await asyncio.sleep(0.5)

    assert before
    with pytest.raises(KeyNotFoundError):
        await kv.get("ui.file.scenes.yaml")


async def test_ui_file_deleted_on_the_leader_is_deleted_on_the_standby(
    make_node: NodeFactory, tmp_path: Path
) -> None:
    """A deleted file is published: otherwise a takeover applies the old content."""
    a = await make_ui_node(make_node, tmp_path, "node_a")
    b = await make_ui_node(make_node, tmp_path, "node_b")
    ui = a.cluster.storage.ui
    await ui._async_check_files(force=True)
    await a.cluster.storage.async_flush()
    path_b = Path(b.hass.config.path("scenes.yaml"))
    assert path_b.exists()

    await a.hass.async_add_executor_job(Path(a.hass.config.path("scenes.yaml")).unlink)
    await ui._async_check_files(force=False)
    await a.cluster.storage.async_flush()

    await wait_for(lambda: not path_b.exists(), timeout=10)
    # Nothing is published again while it stays deleted.
    await ui._async_check_files(force=False)
    assert not ui.pending
