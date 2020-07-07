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

from connection import Connection
from protocol import StatsdProtocol
from protocol import DogStatsdProtocol
from transport_layer_protocol import ProtocolFlag


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
            read_timeout: float = 0.5,
            loop: Optional['asyncio.get_event_loop'] = None
    ) -> NoReturn:
        self._queue: asyncio.Queue = asyncio.Queue()
        self._listen_future: Optional[asyncio.Future] = None
        self._join_future: Optional[asyncio.Future] = None

        self._closing: bool = True
        self._read_timeout: float = read_timeout
        self._close_timeout: float = close_timeout
        self.connection: 'Connection' = Connection(
            host,
            port,
            protocol,
            debug,
            timeout,
            create_timeout,
            close_timeout,
            loop if loop else asyncio.get_event_loop()
        )

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, *args) -> NoReturn:
        await self.close()

    async def connect(self) -> NoReturn:
        await self.connection.connect()
        self._closing = False
        self._queue = asyncio.Queue()
        self._listen_future = asyncio.ensure_future(self._listen())
        self._join_future = asyncio.Future()
        logging.info(f'create aiostatsd client{self}')

    async def close(self) -> NoReturn:
        self._closing = True

        try:
            await asyncio.wait_for(self._close(), timeout=self._close_timeout)
            logging.info(f'close aiostatsd {self}')
        except asyncio.TimeoutError:
            pass

    async def _close(self) -> NoReturn:
        await self._join_future
        self._listen_future.cancel()
        self._listen_future = None
        self._join_future = None
        await self.connection.close()

    async def _listen(self) -> NoReturn:
        try:
            while not self._closing:
                if not self._queue.empty():
                    await self._real_send()
                await asyncio.sleep(0.01)
        except Exception as e:
            logging.error(f'aiostatsd listen error: {e}')
        finally:
            while not self._queue.empty():
                await self._real_send()
            self._join_future.set_result(True)

    async def _real_send(self) -> NoReturn:
        coro = self._queue.get()
        try:
            msg = await asyncio.wait_for(coro, timeout=self._read_timeout)
        except asyncio.TimeoutError:
            logging.error(f'aiostatsd get by queue timeout')
        else:
            try:
                self.connection.sendto(msg)
            except Exception as e:
                logging.error(
                    f'connection:{self.connection}'
                    f' send msd error:{e}, drop msg:{msg}'
                )

    def send(self, msg: str) -> NoReturn:
        try:
            self._queue.put_nowait(msg)
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
            read_timeout: float = 0.5,
            loop: Optional['asyncio.get_event_loop'] = None
    ) -> NoReturn:
        super().__init__(
            host=host,
            port=port,
            protocol=protocol,
            timeout=timeout,
            debug=debug,
            close_timeout=close_timeout,
            create_timeout=create_timeout,
            read_timeout=read_timeout,
            loop=loop
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
            read_timeout: float = 0.5,
            loop: Optional['asyncio.get_event_loop'] = None
    ) -> NoReturn:
        super().__init__(
            host=host,
            port=port,
            protocol=protocol,
            timeout=timeout,
            debug=debug,
            close_timeout=close_timeout,
            create_timeout=create_timeout,
            read_timeout=read_timeout,
            loop=loop
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
            if sample_rate != 1 and random() > sample_rate:
                msg += f'|@{sample_rate}'
            else:
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
        _loop = asyncio.get_event_loop()
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
            tag_dict: Optional[dict] = None,

            timeout: int = 0,
            debug: bool = False,
            close_timeout: int = 5,
            create_timeout: int = 5,
            read_timeout: float = 0.5,
            loop: Optional['asyncio.get_event_loop'] = None
    ) -> NoReturn:
        super().__init__(
            host=host,
            port=port,
            protocol=protocol,
            timeout=timeout,
            debug=debug,
            close_timeout=close_timeout,
            create_timeout=create_timeout,
            read_timeout=read_timeout,
            loop=loop
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
