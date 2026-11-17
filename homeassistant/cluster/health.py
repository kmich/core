"""An HTTP endpoint that says whether this node is the cluster's leader.

``GET /api/cluster/leader`` answers ``200`` on the node that leads and
``503`` on every other node: also on a new leader until its promotion set up
the clustered entries (one after another, which takes minutes on a large
installation), on a node that is not connected to NATS, and on one that is
shutting down. Managed add-ons are restored and started in the background
once the node leads, and may still be starting when it answers ``200``: a
proxy for an add-on's port (MQTT) should also check that port. A load balancer, a reverse proxy or a VRRP daemon
(keepalived) checks it on every node and sends traffic, or moves a virtual
IP, to the one that answers ``200``: inbound connections then follow the
leader (the UI, webhooks, an alarm panel that reports over SIA, MQTT devices
that connect to the broker of a managed add-on).

The endpoint needs no authentication: health checks of proxies and VRRP
daemons cannot log in. It tells only whether this node leads, nothing else.
"""

from http import HTTPStatus
from typing import TYPE_CHECKING, Final

from aiohttp import web

from homeassistant.core import HomeAssistant
from homeassistant.helpers.http import HomeAssistantView

if TYPE_CHECKING:
    from . import ClusterNode

LEADER_URL: Final = "/api/cluster/leader"


class ClusterLeaderView(HomeAssistantView):
    """``200`` on the leader, ``503`` elsewhere."""

    url = LEADER_URL
    name = "api:cluster:leader"
    requires_auth = False

    def __init__(self, node: ClusterNode) -> None:
        """Initialize with this node."""
        self.node = node

    async def get(self, request: web.Request) -> web.Response:
        """Answer the health check."""
        leading = self.node.serves_as_leader
        return self.json(
            {"leader": leading},
            HTTPStatus.OK if leading else HTTPStatus.SERVICE_UNAVAILABLE,
            headers={"Cache-Control": "no-store"},
        )

    async def head(self, request: web.Request) -> web.Response:
        """Answer the health check without a body."""
        leading = self.node.serves_as_leader
        return web.Response(
            status=HTTPStatus.OK if leading else HTTPStatus.SERVICE_UNAVAILABLE,
            headers={"Cache-Control": "no-store"},
        )


async def async_register_leader_view(hass: HomeAssistant, node: ClusterNode) -> None:
    """Register the endpoint (once ``http`` is set up)."""
    hass.http.register_view(ClusterLeaderView(node))
