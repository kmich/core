"""Every in-process test node has a ``.storage`` of its own."""

from homeassistant.helpers.storage import Store

from .conftest import NodeFactory


async def test_nodes_do_not_share_a_disk(make_node: NodeFactory) -> None:
    """What one node saves is not on the disk of another.

    With one shared dictionary a standby could "receive" what the leader saved
    without the cluster sending it anything, and a test of the sync would pass.
    """
    a = await make_node("node_a")
    b = await make_node("node_b")

    await Store(a.hass, 1, "probe").async_save({"only": "on a"})

    assert "probe" in make_node.disks[a.hass]
    assert "probe" not in make_node.disks[b.hass]
    assert await Store(b.hass, 1, "probe").async_load() is None
    assert await Store(a.hass, 1, "probe").async_load() == {"only": "on a"}
