import asyncio
import time
from typing import AsyncGenerator

import pytest

import aio_statsd

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def graphite_client() -> AsyncGenerator[aio_statsd.GraphiteClient, None]:
    client: aio_statsd.GraphiteClient = aio_statsd.GraphiteClient(port=9999)
    await client.connect()
    yield client
    await client.close()


class TestGraphiteClient:
    async def test_send(self, graphite_client: aio_statsd.GraphiteClient, udp_server: asyncio.Queue) -> None:
        now_timestamp: int = int(time.time())
        interval: int = 10
        timestamp: int = int(now_timestamp) // interval * interval
        graphite_client.send_graphite("test.key", 1, timestamp=timestamp, interval=interval)
        assert await udp_server.get() == str.encode(f"test.key 1 {timestamp}")
