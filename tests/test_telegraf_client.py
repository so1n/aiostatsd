import asyncio
from typing import Any, AsyncGenerator

import pytest

import aio_statsd

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def telegraf_client() -> AsyncGenerator[aio_statsd.TelegrafClient, None]:
    client: aio_statsd.TelegrafClient = aio_statsd.TelegrafClient(port=9999)
    await client.connect()
    yield client
    await client.close()


class TestTelegrafClient:
    async def test_field(
        self, telegraf_client: aio_statsd.TelegrafClient, udp_server: asyncio.Queue, mocker: Any
    ) -> None:
        demo_timestamp: int = 1_600_000_000 * 1000 * 1000 * 1000
        mocker.patch("aio_statsd.client.time.time", return_value=1_600_000_000)
        telegraf_client.send_telegraf("test.key", {"value": 1, "str": "test", "bool": True})
        assert await udp_server.get() == str.encode(f'test.key value=1i,str="test",bool=True {demo_timestamp}')

    async def test_tag(
        self, telegraf_client: aio_statsd.TelegrafClient, udp_server: asyncio.Queue, mocker: Any
    ) -> None:
        demo_timestamp: int = 1_600_000_000 * 1000 * 1000 * 1000
        mocker.patch("aio_statsd.client.time.time", return_value=1_600_000_000)
        telegraf_client.send_telegraf("test.key", {"value": 1}, tag_dict={"tag": "1"})
        assert await udp_server.get() == str.encode(f"test.key,tag=1 value=1i {demo_timestamp}")

    async def test_use_server_time(self, telegraf_client: aio_statsd.TelegrafClient, udp_server: asyncio.Queue) -> None:
        telegraf_client.send_telegraf("test.key", {"value": 1}, tag_dict={"tag": "1"}, user_server_time=True)
        assert await udp_server.get() == str.encode(f"test.key,tag=1 value=1i")

    async def test_special_char(
        self, telegraf_client: aio_statsd.TelegrafClient, udp_server: asyncio.Queue, mocker: Any
    ) -> None:
        demo_timestamp: int = 1_600_000_000 * 1000 * 1000 * 1000
        mocker.patch("aio_statsd.client.time.time", return_value=1_600_000_000)
        telegraf_client.send_telegraf(
            "test.key",
            {"value": 1},
            tag_dict={"ta,g": "1", "tag2": "hello word", "tag3": "a=b"},
            enable_replace_special=True,
        )
        assert await udp_server.get() == str.encode(
            f"test.key,ta\\,g=1,tag2=hello\\ word,tag3=a\\=b value=1i {demo_timestamp}"
        )

    async def test_tag_key_error(self, telegraf_client: aio_statsd.TelegrafClient) -> None:
        with pytest.raises(RuntimeError):
            telegraf_client.send_telegraf("test.key", {"value": 1}, tag_dict={"_field": "1"})
