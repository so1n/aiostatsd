#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-02'
import asyncio
import logging
import time
from contextlib import contextmanager
from random import random
from typing import Iterator, NoReturn, Optional, Union

from protocol import StatsdProtocol
from udpconnection import UdpConnection

LOGGER = logging.getLogger()


class Client:

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8125,
        read_timeout: float = 0.5,
        close_timeout: Optional[float] = None,
        sample_rate: Union[float, int] = 1,
    ) -> NoReturn:
        self._queue: asyncio.Queue = asyncio.Queue()
        self._listen_future: Optional[asyncio.Future] = None
        self._join_future: Optional[asyncio.Future] = None

        self._closing = True
        self._read_timeout = read_timeout
        self._close_timeout = close_timeout
        self._sample_rate = sample_rate
        self.udp_connection = UdpConnection(host, port)

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, *args) -> NoReturn:
        await self.close()

    def __or__(self, other):
        pass

    async def connect(self) -> NoReturn:
        await self.udp_connection.connect()
        self._closing = False
        self._queue = asyncio.Queue()
        self._listen_future = asyncio.ensure_future(self._listen())
        self._join_future = asyncio.Future()

    async def close(self) -> NoReturn:
        self._closing = True

        try:
            await asyncio.wait_for(self._close(), timeout=self._close_timeout)
        except asyncio.TimeoutError:
            pass

    async def _close(self) -> NoReturn:
        await self._join_future
        self._listen_future.cancel()
        self._listen_future = None
        self._join_future = None
        await self.udp_connection.close()

    async def _listen(self) -> NoReturn:
        try:
            while not self._closing:
                await self._send()
        finally:
            while not self._queue.empty():
                await self._send()
            self._join_future.set_result(True)

    async def _send(self) -> NoReturn:
        coro = self._queue.get()
        try:
            msg = await asyncio.wait_for(coro, timeout=self._read_timeout)
        except asyncio.TimeoutError:
            pass
        else:
            self.udp_connection.sendto(msg)

    def send(self, msg: str) -> NoReturn:
        self._queue.put_nowait(msg)

    def send_graphite(
            self,
            key: str,
            value: int = 0,
            timestamp: int = time.time(),
            interval: int = 10
    ) -> NoReturn:
        """interval: Multiple clients timestamp interval synchronization"""
        timestamp = int(timestamp) // interval * interval
        msg = "{} {} {}".format(key, value, timestamp)
        self.send(msg)

    def send_statsd(
            self,
            statsd_protocol: 'StatsdProtocol',
            sample_rate: Union[int, float, None] = None
    ) -> NoReturn:
        msg = statsd_protocol.msg
        if '\n' in msg:
            self.send(msg)
            if sample_rate:
                LOGGER.warning('Multi-Metric not support sample rate')
        else:
            sample_rate = sample_rate or self._sample_rate
            if sample_rate != 1 and random() > sample_rate:
                msg += f'|@{sample_rate}'
                self.send(msg)

    def counter(
            self,
            key: str,
            value: int,
            sample_rate: Union[int, float, None] = None
    ) -> NoReturn:
        statsd_protocol = StatsdProtocol().counter(key, value)
        return self.send_statsd(statsd_protocol, sample_rate)

    def timer(
            self,
            key: str,
            value: Union[int, float],
            sample_rate: Union[int, float, None] = None
    ) -> NoReturn:
        statsd_protocol = StatsdProtocol().timer(key, value)
        return self.send_statsd(statsd_protocol, sample_rate)

    def gauge(
            self,
            key: str,
            value: str,
            sample_rate: Union[int, float, None] = None
    ) -> NoReturn:
        statsd_protocol = StatsdProtocol().gauge(key, value)
        return self.send_statsd(statsd_protocol, sample_rate)

    def sets(
            self,
            key: str,
            value: int,
            sample_rate: Union[int, float, None] = None
    ) -> NoReturn:
        statsd_protocol = StatsdProtocol().sets(key, value)
        return self.send_statsd(statsd_protocol, sample_rate)

    @contextmanager
    def timeit(
            self,
            key: str,
            sample_rate: Union[int, float, None] = None
    ) -> Iterator[None]:
        """
        Context manager for easily timing methods.
        """
        _loop = asyncio.get_event_loop()
        started_at = _loop.time()
        yield
        value = (_loop.time() - started_at) * 1000
        self.timer(key, value, sample_rate)
