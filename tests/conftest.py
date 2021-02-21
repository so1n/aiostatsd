import asyncio

import pytest


@pytest.fixture
async def udp_server():
    result_queue: asyncio.Queue = asyncio.Queue()

    class ServerProtocol(asyncio.DatagramProtocol):

        def datagram_received(self, data, addr):
            result_queue.put_nowait(data)

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(),
        local_addr=('127.0.0.1', 9999)
    )

    yield result_queue

    transport.close()


@pytest.fixture
async def tcp_server():
    result_queue: asyncio.Queue = asyncio.Queue()

    class ServerProtocol(asyncio.Protocol):
        def data_received(self, data):
            result_queue.put_nowait(data)

    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    server = await loop.create_server(
        lambda: ServerProtocol(), '127.0.0.1', 9999
    )

    yield result_queue

    server.close()
    await server.wait_closed()

