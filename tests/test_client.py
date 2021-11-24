import asyncio
from typing import AsyncGenerator

import pytest

import aio_statsd

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def statsd_client() -> AsyncGenerator[aio_statsd.StatsdClient, None]:
    client: aio_statsd.StatsdClient = aio_statsd.StatsdClient(port=9999)
    await client.connect()
    yield client
    await client.close()


class TestClient:
    async def test_enable_debug(self, udp_server: asyncio.Queue) -> None:
        client: aio_statsd.StatsdClient = aio_statsd.StatsdClient(port=9999, debug=True)
        await client.connect()
        client.counter("test.key", 1)
        assert await udp_server.get() == b"test.key:1|c"
        await client.close()

    async def test_tcp_server(self, tcp_server: asyncio.Queue) -> None:
        client: aio_statsd.GraphiteClient = aio_statsd.GraphiteClient(
            port=9999, protocol=aio_statsd.ProtocolFlag.tcp, timeout=9
        )
        await client.connect()
        client.send("test")
        assert await tcp_server.get() == b"test"
        await client.close()

    async def test_error_transport_layer_protocol(self) -> None:
        with pytest.raises(ConnectionError) as e:
            client: aio_statsd.StatsdClient = aio_statsd.StatsdClient(port=9999, protocol="error")  # type:ignore
            await client.connect()

        exec_msg = e.value.args[0]
        assert exec_msg == "Not support protocol:error"

    async def test_client_already_connected(self, statsd_client: aio_statsd.StatsdClient) -> None:
        with pytest.raises(ConnectionError) as e:
            await statsd_client.connect()

        exec_msg = e.value.args[0]
        assert exec_msg == "StatsdClient already connected"

    async def test_client_context_manager(self, udp_server: asyncio.Queue) -> None:
        async with aio_statsd.StatsdClient(port=9999) as client:
            client.counter("test.key", 1)  # type:ignore
            assert await udp_server.get() == b"test.key:1|c"

            # test close client contenxt manager until send all msg
            for i in range(100000):
                client.counter("test.key", i)  # type:ignore

    async def test_connection_refused(self) -> None:
        with pytest.raises(ConnectionError) as e:
            client: aio_statsd.StatsdClient = aio_statsd.StatsdClient(port=9999, timeout=1)
            await client.connect()
            client.counter("test.key", 1)
            await asyncio.sleep(1)
            client.counter("test.key", 1)

            await asyncio.sleep(1)
            client.counter("test.key", 1)

            await asyncio.sleep(1)
            client.counter("test.key", 1)
            await client.close()

        exec_msg = e.value.args[0]
        assert exec_msg == "StatsdClient already close."

    async def test_protocol_send_msg_timeout(self, udp_server: asyncio.Queue) -> None:
        """TODO ...."""
        client: aio_statsd.StatsdClient = aio_statsd.StatsdClient(port=9999, timeout=1)
        await client.connect()
        client.counter("test.key", 1)
        client.counter("test.key", 1)
        await client.close()
