import asyncio
from typing import Any, AsyncGenerator

import pytest

import aio_statsd

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def telegraf_statsd_client() -> AsyncGenerator[aio_statsd.TelegrafStatsdClient, None]:
    client: aio_statsd.TelegrafStatsdClient = aio_statsd.TelegrafStatsdClient(port=9999)
    await client.connect()
    yield client
    await client.close()


class TestTelegrafStatsdClient:
    async def test_set(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.set("test.key", 1)
        assert await udp_server.get() == b"test.key:1|s"

    async def test_counter(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.counter("test.key", 1)
        assert await udp_server.get() == b"test.key:1|c"

    async def test_increment(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.increment("test.key", 1)
        assert await udp_server.get() == b"test.key:+1|g"

    async def test_decrement(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.decrement("test.key", 1)
        assert await udp_server.get() == b"test.key:-1|g"

    async def test_gauge(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.gauge("test.key", 1)
        assert await udp_server.get() == b"test.key:1|g"

    async def test_timer(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.timer("test.key", 1)
        assert await udp_server.get() == b"test.key:1|ms"

    async def test_distribution(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.distribution("test.key", 1)
        assert await udp_server.get() == b"test.key:1|d"

    async def test_histogram(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.histogram("test.key", 1)
        assert await udp_server.get() == b"test.key:1|h"

    async def test_tag(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        telegraf_statsd_client.set("test.key", 1, tag_dict={"version": "1", "author": "so1n"})
        assert await udp_server.get() == b"test.key,version=1,author=so1n:1|s"

    async def test_namespace(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        metric = aio_statsd.TelegrafStatsdProtocol(prefix="so1n_test")
        metric.gauge("test2.key", 1)
        metric.increment("test2.key", 1)
        telegraf_statsd_client.send_telegraf_statsd(metric)

        for result in [b"so1n_test.test2.key:1|g", b"so1n_test.test2.key:+1|g"]:
            assert await udp_server.get() == result

    async def test_timeit(
        self, mocker: Any, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        loop = mocker.patch("aio_statsd.client.get_event_loop")
        loop.return_value.time.return_value = 1.0
        with telegraf_statsd_client.timeit("test.key"):
            loop.return_value.time.return_value = 2.0

        assert await udp_server.get() == b"test.key:1000|ms"

    async def test_mutli_msg(
        self, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient, udp_server: asyncio.Queue
    ) -> None:
        metric = aio_statsd.TelegrafStatsdProtocol()
        metric.gauge("test2.key", 1)
        metric.increment("test2.key", 1)
        telegraf_statsd_client.send_telegraf_statsd(metric)

        for result in [b"test2.key:1|g", b"test2.key:+1|g"]:
            assert await udp_server.get() == result

    async def test_sample_rate(self, mocker: Any, telegraf_statsd_client: aio_statsd.TelegrafStatsdClient) -> None:
        telegraf_statsd_client_deque = telegraf_statsd_client._deque
        mocked_deque = mocker.patch.object(telegraf_statsd_client, "_deque")
        telegraf_statsd_client.increment("test.key", 1, sample_rate=1)
        mocked_deque.appendleft.assert_called_once_with("test.key:+1|g")

        mocked_deque = mocker.patch.object(telegraf_statsd_client, "_deque")
        mocker.patch("aio_statsd.client.random", return_value=0.9)
        telegraf_statsd_client.increment("test.key", 2, sample_rate=0.8)
        mocked_deque.appendleft.assert_called_once_with("test.key:+2|g|@0.8")

        mocked_deque = mocker.patch.object(telegraf_statsd_client, "_deque")
        mocker.patch("aio_statsd.client.random", return_value=0.3)
        telegraf_statsd_client.increment("test.key", 3, sample_rate=0.5)
        mocked_deque.appendleft.assert_not_called()
        telegraf_statsd_client._deque = telegraf_statsd_client_deque
