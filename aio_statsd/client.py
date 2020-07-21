#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-02'
import asyncio
import logging
import time
from collections import deque
from contextlib import contextmanager
from random import random
from typing import Iterator, NoReturn, Optional, Union

from aio_statsd.connection import Connection
from aio_statsd.pool import Pool
from aio_statsd.protocol import StatsdProtocol
from aio_statsd.protocol import DogStatsdProtocol
from aio_statsd.transport_layer_protocol import ProtocolFlag
from aio_statsd.utlis import get_event_loop


class Client:

    def __init__(
            self,
            host: str = "localhost",
            port: int = 8125,
            protocol: ProtocolFlag = ProtocolFlag.udp,

            timeout: int = 0,
            debug: bool = False,
            close_timeout: int = 5,
            create_timeout: int = 5,
            max_len: int = 1000,
    ) -> NoReturn:
        self._queue_empty = object()
        self._max_len = max_len
        self._queue: Optional[deque] = None
        self._listen_future: Optional[asyncio.Future] = None
        self._join_future: Optional[asyncio.Future] = None

        self._is_listen: bool = False
        self._close_timeout: float = close_timeout
        self._conn_config_dict = {
            'host': host,
            'port': port,
            'protocol_flag': protocol,
            'debug': debug,
            'timeout': timeout,
            'create_timeout': create_timeout,
        }

        self.connection: Union[Pool, Connection, None] = None

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, *args) -> NoReturn:
        await self.close()

    @property
    def is_closed(self):
        return not (self.connection and not self.connection.is_closed)

    async def connect(self) -> NoReturn:
        if not self.is_closed:
            raise ConnectionError(f'aiostatsd client already connected')
        self.connection = Connection(**self._conn_config_dict)
        await self._connect()

    async def create_pool(self, min_size: int = 1, max_size: int = 10):
        if not self.is_closed:
            raise ConnectionError(f'aiostatsd client already connected')
        self.connection = Pool(
            **self._conn_config_dict,
            min_size=min_size,
            max_size=max_size
        )
        await self._connect()

    async def _connect(self):
        await self.connection.connect()
        self._queue = deque(maxlen=self._max_len)
        self._is_listen = True
        self._listen_future = asyncio.ensure_future(self._listen())
        logging.info(f'create aiostatsd client{self}')

    async def close(self) -> NoReturn:
        self._is_listen = False
        await self._listen_future
        self._listen_future = None

    async def _close(self):
        if not self.is_closed:
            try:
                await asyncio.wait_for(self._before_close(), timeout=self._close_timeout)
            except asyncio.TimeoutError:
                pass
            await self.connection.await_close()

    def _get_by_queue(self) -> Union[object, str]:
        try:
            return self._queue.pop()
        except IndexError:
            return self._queue_empty

    async def _before_close(self):
        while True:
            value = self._get_by_queue()
            if value is not self._queue_empty:
                await self._real_send(value)
            else:
                break

    async def _listen(self) -> NoReturn:
        try:
            while self._is_listen:
                value = self._get_by_queue()
                if value is not self._queue_empty:
                    await self._real_send(value)
                else:
                    await asyncio.sleep(0.01)
        except Exception as e:
            logging.error(f'aiostatsd listen status:{self._is_listen} error: {e}')
        finally:
            await self._close()

    async def _real_send(self, msg: str) -> NoReturn:
        try:
            self.connection.sendto(msg)
        except Exception as e:
            logging.error(f'connection:{self.connection}')
            if len(self._queue) < self._queue.maxlen * 0.9:
                self._queue.append(msg)
                await asyncio.sleep(1)
            else:
                logging.error(f'send msd error:{e}, drop msg:{msg}')

    def send(self, msg: str) -> NoReturn:
        try:
            # if queue full, auto del last value(queue[-1])
            self._queue.appendleft(msg)
        except Exception as e:
            logging.error(f'aiostatsd put:{msg} to queue error:{e}')


class GraphiteClient(Client):

    def __init__(
            self,
            host: str = "localhost",
            port: int = 2003,
            protocol: ProtocolFlag = ProtocolFlag.udp,

            timeout: int = 0,
            debug: bool = False,
            close_timeout: int = 5,
            create_timeout: int = 5,
            max_len: int = 10000,
    ) -> NoReturn:
        super().__init__(
            host=host,
            port=port,
            protocol=protocol,
            timeout=timeout,
            debug=debug,
            close_timeout=close_timeout,
            create_timeout=create_timeout,
            max_len=max_len,
        )

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


class StatsdClient(Client):

    def __init__(
            self,
            host: str = "localhost",
            port: int = 8125,
            protocol: ProtocolFlag = ProtocolFlag.udp,
            sample_rate: Union[float, int] = 1,

            timeout: int = 0,
            debug: bool = False,
            close_timeout: int = 5,
            create_timeout: int = 5,
            max_len: int = 10000,
    ) -> NoReturn:
        super().__init__(
            host=host,
            port=port,
            protocol=protocol,
            timeout=timeout,
            debug=debug,
            close_timeout=close_timeout,
            create_timeout=create_timeout,
            max_len=max_len,
        )
        self._sample_rate = sample_rate

    def send_statsd(
            self,
            statsd_protocol: 'StatsdProtocol',
            sample_rate: Union[int, float, None] = None
    ) -> NoReturn:
        msg = statsd_protocol.msg
        if '\n' in msg and sample_rate:
            logging.warning('Multi-Metric not support sample rate')
        else:
            sample_rate = sample_rate or self._sample_rate
            if random() > sample_rate:
                msg += f'|@{sample_rate}'
            elif sample_rate > 1:
                return
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
            value: int,
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
        _loop = get_event_loop()
        started_at = _loop.time()
        yield
        value = (_loop.time() - started_at) * 1000
        self.timer(key, value, sample_rate)


class DogStatsdClient(Client):

    def __init__(
            self,
            host: str = "localhost",
            port: int = 8125,
            protocol: ProtocolFlag = ProtocolFlag.udp,
            sample_rate: Union[float, int] = 1,

            timeout: int = 0,
            debug: bool = False,
            close_timeout: int = 5,
            create_timeout: int = 5,
            max_len: int = 10000
    ) -> NoReturn:
        super().__init__(
            host=host,
            port=port,
            protocol=protocol,
            timeout=timeout,
            debug=debug,
            close_timeout=close_timeout,
            create_timeout=create_timeout,
            max_len=max_len,
        )
        self._sample_rate = sample_rate

    def send_dog_statsd(
            self,
            dog_statsd_protocol: 'DogStatsdProtocol',
            sample_rate: Union[int, float, None] = None
    ) -> NoReturn:
        for msg in dog_statsd_protocol.get_msg_list():
            sample_rate = sample_rate or self._sample_rate
            if sample_rate != 1 and random() > sample_rate:
                msg += f'|@{sample_rate}'
            else:
                continue
            self.send(msg)

    def gauge(
            self,
            key: str,
            value: int,
            sample_rate: Union[int, float, None] = None,
            tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol = DogStatsdProtocol().gauge(key, value, tag_dict)
        return self.send_dog_statsd(protocol, sample_rate)

    def increment(
            self,
            key: str,
            value: int,
            sample_rate: Union[int, float, None] = None,
            tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol = DogStatsdProtocol().increment(key, value, tag_dict)
        return self.send_dog_statsd(protocol, sample_rate)

    def decrement(
            self,
            key: str,
            value: int,
            sample_rate: Union[int, float, None] = None,
            tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol = DogStatsdProtocol().increment(key, value, tag_dict)
        return self.send_dog_statsd(protocol, sample_rate)

    def timer(
            self,
            key: str,
            value: Union[int, float],
            sample_rate: Union[int, float, None] = None,
            tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol = DogStatsdProtocol().timer(key, value, tag_dict)
        return self.send_dog_statsd(protocol, sample_rate)

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

    def histogram(
            self,
            key: str,
            value: Union[int, float],
            sample_rate: Union[int, float, None] = None,
            tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol = DogStatsdProtocol().histogram(key, value, tag_dict)
        return self.send_dog_statsd(protocol, sample_rate)

    def distribution(
            self,
            key: str,
            value: Union[int, float],
            sample_rate: Union[int, float, None] = None,
            tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol = DogStatsdProtocol().distribution(key, value, tag_dict)
        return self.send_dog_statsd(protocol, sample_rate)
