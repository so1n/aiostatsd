import asyncio
from typing import AsyncGenerator, Tuple

import pytest


@pytest.fixture
async def udp_server() -> AsyncGenerator[asyncio.Queue, None]:
    result_queue: asyncio.Queue = asyncio.Queue()

    class ServerProtocol(asyncio.DatagramProtocol):
        def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
            result_queue.put_nowait(data)

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    transport, protocol = await loop.create_datagram_endpoint(lambda: ServerProtocol(), local_addr=("127.0.0.1", 9999))

    yield result_queue

    transport.close()


@pytest.fixture
async def tcp_server() -> AsyncGenerator[asyncio.Queue, None]:
    result_queue: asyncio.Queue = asyncio.Queue()

    class ServerProtocol(asyncio.Protocol):
        def data_received(self, data: bytes) -> None:
            result_queue.put_nowait(data)

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    server = await loop.create_server(lambda: ServerProtocol(), "127.0.0.1", 9999)

    yield result_queue

    server.close()
    await server.wait_closed()
