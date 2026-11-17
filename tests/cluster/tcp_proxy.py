"""A TCP proxy that can silently blackhole traffic, to simulate a network drop.

Blackholing (rather than closing sockets) is the nasty case: no RST, no FIN,
the client only notices through timeouts, exactly like a pulled cable.
"""

import asyncio
import contextlib


class BlackholeProxy:
    """Forward 127.0.0.1:<port> to an upstream; ``cut()`` drops all bytes."""

    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        """Initialize."""
        self.upstream = (upstream_host, upstream_port)
        self.blackholed = False
        self.port = 0
        self._server: asyncio.base_events.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        """Listen on a free local port."""
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    @property
    def url(self) -> str:
        """NATS URL that goes through the proxy."""
        return f"nats://127.0.0.1:{self.port}"

    def cut(self) -> None:
        """Drop every byte from now on, without closing anything."""
        self.blackholed = True

    def restore(self) -> None:
        """Forward bytes again."""
        self.blackholed = False

    async def stop(self) -> None:
        """Close the listener."""
        if self._server:
            self._server.close()
        for writer in list(self._writers):
            writer.close()
        if self._server:
            with contextlib.suppress(Exception):
                await self._server.wait_closed()

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if self.blackholed:
            # Accept and never answer, like a dead peer behind a switch.
            self._writers.add(writer)
            return
        up_reader, up_writer = await asyncio.open_connection(*self.upstream)
        self._writers.update((writer, up_writer))

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            with contextlib.suppress(Exception):
                while data := await src.read(65536):
                    if self.blackholed:
                        continue
                    dst.write(data)
                    await dst.drain()
            dst.close()

        await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))
