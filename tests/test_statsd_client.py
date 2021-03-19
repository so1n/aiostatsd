import asyncio
from typing import Any, AsyncGenerator

import pytest

import aio_statsd

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def statsd_client() -> AsyncGenerator[aio_statsd.StatsdClient, None]:
    client: aio_statsd.StatsdClient = aio_statsd.StatsdClient(port=9999)
    await client.connect()
    yield client
    await client.close()


class TestStatsdClient:
    async def test_counter(self, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        statsd_client.counter("test.key", 1)
        assert await udp_server.get() == b"test.key:1|c"

    async def test_gauge(self, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        statsd_client.gauge("test.key", 1)
        assert await udp_server.get() == b"test.key:1|g"

    async def test_sets(self, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        statsd_client.sets("test.key", 1)
        assert await udp_server.get() == b"test.key:1|s"

    async def test_timer(self, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        statsd_client.timer("test.key", 1)
        assert await udp_server.get() == b"test.key:1|ms"

    async def test_increment(self, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        statsd_client.increment("test.key", 1)
        assert await udp_server.get() == b"test.key:+1|g"

    async def test_decrement(self, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        statsd_client.decrement("test.key", 1)
        assert await udp_server.get() == b"test.key:-1|g"
        
    async def test_timeit(self, mocker: Any, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        loop = mocker.patch("aio_statsd.client.get_event_loop")
        loop.return_value.time.return_value = 1.0
        with statsd_client.timeit("test.key"):
            loop.return_value.time.return_value = 2.0

        assert await udp_server.get() == b"test.key:1000|ms"

    async def test_namespace(self, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        metric = aio_statsd.StatsdProtocol(prefix="so1n_test")
        metric.gauge("test2.key", 1)
        metric.sets("test2.key", 1)
        statsd_client.send_statsd(metric)
        assert await udp_server.get() == b"so1n_test.test2.key:1|g\nso1n_test.test2.key:1|s"

    async def test_mutli_msg(self, statsd_client: aio_statsd.StatsdClient, udp_server: asyncio.Queue) -> None:
        metric = aio_statsd.StatsdProtocol()
        metric.gauge("test2.key", 1)
        metric.sets("test2.key", 1)
        statsd_client.send_statsd(metric)

        assert await udp_server.get() == b"test2.key:1|g\ntest2.key:1|s"

    async def test_mutli_msg_if_msg_length_gt_mtu_length(self, statsd_client: aio_statsd.StatsdClient) -> None:
        with pytest.raises(RuntimeError):
            metric = aio_statsd.StatsdProtocol(mtu_limit=5)
            metric.gauge("test2.key", 1)
            metric.sets("test2.key", 1)
            statsd_client.send_statsd(metric)

    async def test_sample_rate(self, mocker: Any, statsd_client: aio_statsd.StatsdClient) -> None:
        statsd_client_deque = statsd_client._deque
        mocked_deque = mocker.patch.object(statsd_client, "_deque")
        statsd_client.counter("test.key", 1, sample_rate=1)
        mocked_deque.appendleft.assert_called_once_with("test.key:1|c")

        mocked_deque = mocker.patch.object(statsd_client, "_deque")
        statsd_client.counter("test.key", 1, sample_rate=2)
        mocked_deque.appendleft.assert_not_called()

        mocked_deque = mocker.patch.object(statsd_client, "_deque")
        mocker.patch("aio_statsd.client.random", return_value=0.6)
        statsd_client.counter("test.key", 1, sample_rate=0.3)
        mocked_deque.appendleft.assert_called_once_with("test.key:1|c|@0.3")

        mocked_deque = mocker.patch.object(statsd_client, "_deque")
        mocker.patch("aio_statsd.client.random", return_value=0.3)
        statsd_client.counter("test.key", 2, sample_rate=0.5)
        mocked_deque.appendleft.assert_not_called()
        statsd_client._deque = statsd_client_deque
