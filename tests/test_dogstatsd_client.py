import pytest

import aio_statsd

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def dog_statsd_client():
    client: aio_statsd.DogStatsdClient = aio_statsd.DogStatsdClient(port=9999)
    await client.connect()
    yield client
    await client.close()


class TestDogStatsdClient:
    async def test_set(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        dog_statsd_client.set("test.key", 1)
        assert await udp_server.get() == b"test.key:1|s"

    async def test_increment(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        dog_statsd_client.increment("test.key", 1)
        assert await udp_server.get() == b"test.key:1|c"

    async def test_decrement(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        dog_statsd_client.decrement("test.key", 1)
        assert await udp_server.get() == b"test.key:-1|c"

    async def test_gauge(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        dog_statsd_client.gauge("test.key", 1)
        assert await udp_server.get() == b"test.key:1|g"

    async def test_timer(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        dog_statsd_client.timer("test.key", 1)
        assert await udp_server.get() == b"test.key:1|ms"

    async def test_distribution(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        dog_statsd_client.distribution("test.key", 1)
        assert await udp_server.get() == b"test.key:1|d"

    async def test_histogram(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        dog_statsd_client.histogram("test.key", 1)
        assert await udp_server.get() == b"test.key:1|h"

    async def test_tag(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        dog_statsd_client.set("test.key", 1, tag_dict={"version": "1", "author": "so1n"})
        assert await udp_server.get() == b"test.key:1|s|#version:1,author:so1n"

    async def test_namespace(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        metric = aio_statsd.DogStatsdProtocol(prefix="so1n_test")
        metric.gauge("test2.key", 1)
        metric.increment("test2.key", 1)
        dog_statsd_client.send_dog_statsd(metric)

        for result in [b"so1n_test.test2.key:1|g", b"so1n_test.test2.key:1|c"]:
            assert await udp_server.get() == result

    async def test_timeit(self, mocker, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        loop = mocker.patch("aio_statsd.client.get_event_loop")
        loop.return_value.time.return_value = 1.0
        with dog_statsd_client.timeit("test.key"):
            loop.return_value.time.return_value = 2.0

        assert await udp_server.get() == b"test.key:1000|ms"

    async def test_mutli_msg(self, dog_statsd_client: aio_statsd.DogStatsdClient, udp_server):
        metric = aio_statsd.DogStatsdProtocol()
        metric.gauge("test2.key", 1)
        metric.increment("test2.key", 1)
        dog_statsd_client.send_dog_statsd(metric)

        for result in [b"test2.key:1|g", b"test2.key:1|c"]:
            assert await udp_server.get() == result

    async def test_sample_rate(self, mocker, dog_statsd_client: aio_statsd.DogStatsdClient):
        dog_statsd_client_deque = dog_statsd_client._deque
        mocked_deque = mocker.patch.object(dog_statsd_client, "_deque")
        dog_statsd_client.increment("test.key", 1, sample_rate=1)
        mocked_deque.appendleft.assert_called_once_with("test.key:1|c")

        mocked_deque = mocker.patch.object(dog_statsd_client, "_deque")
        mocker.patch("aio_statsd.client.random", return_value=0.9)
        dog_statsd_client.increment("test.key", 2, sample_rate=0.8)
        mocked_deque.appendleft.assert_called_once_with("test.key:2|c|@0.8")

        mocked_deque = mocker.patch.object(dog_statsd_client, "_deque")
        mocker.patch("aio_statsd.client.random", return_value=0.3)
        dog_statsd_client.increment("test.key", 3, sample_rate=0.5)
        mocked_deque.appendleft.assert_not_called()
        dog_statsd_client._deque = dog_statsd_client_deque
