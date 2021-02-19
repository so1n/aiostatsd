#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = "so1n"
__date__ = "2020-02"
import asyncio
import logging
import time
from collections import deque
from contextlib import contextmanager
from random import random
from typing import Any, Dict, Iterator, NoReturn, Optional, Union

from aio_statsd.connection import Connection
from aio_statsd.protocol import DogStatsdProtocol, StatsdProtocol
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
        self._queue_empty: "object()" = object()
        self._max_len: int = max_len
        self._deque: Optional[deque] = None
        self._listen_future: Optional[asyncio.Future] = None

        self.is_closed: bool = True
        self._close_timeout: int = close_timeout
        self._conn_config_dict: Dict[str, Any] = {
            "host": host,
            "port": port,
            "protocol_flag": protocol,
            "debug": debug,
            "timeout": timeout,
            "create_timeout": create_timeout,
        }

        self.connection: Optional[Connection] = None

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, *args) -> NoReturn:
        async def await_deque_empty():
            while True:
                if not self._deque:
                    break
                await asyncio.sleep(0.1)

        try:
            await asyncio.wait_for(await_deque_empty(), 9)
        except asyncio.TimeoutError:
            pass
        await self.close()

    async def connect(self) -> NoReturn:
        if not self.is_closed:
            raise ConnectionError(f"aiostatsd client already connected")
        self.connection = Connection(**self._conn_config_dict)
        await self.connection.connect()
        self._deque = deque(maxlen=self._max_len)
        self.is_closed = False
        self._listen_future = asyncio.ensure_future(self._listen())
        logging.info(f"create aiostatsd client{self}")

    async def close(self) -> NoReturn:
        self.is_closed = False
        await self._listen_future
        self._listen_future = None

    async def _close(self):
        if not self.is_closed:

            async def before_close():
                while True:
                    value: Union[object, str] = self._get_by_queue()
                    if value is not self._queue_empty:
                        await self._real_send(value)
                    else:
                        break

            try:
                await asyncio.wait_for(before_close(), timeout=self._close_timeout)
            except asyncio.TimeoutError:
                pass
            await self.connection.await_close()

    def _get_by_queue(self) -> Union[object, str]:
        try:
            return self._deque.pop()
        except IndexError:
            return self._queue_empty

    async def _listen(self) -> NoReturn:
        try:
            while not self.is_closed:
                value: Union[object, str] = self._get_by_queue()
                if value is not self._queue_empty:
                    await self._real_send(value)
                else:
                    await asyncio.sleep(0.01)
        except Exception as e:
            logging.error(f"aiostatsd listen status:{self.is_closed} error: {e}")
        finally:
            await self._close()

    async def _real_send(self, msg: str) -> NoReturn:
        try:
            self.connection.sendto(msg)
        except Exception as e:
            logging.error(f"connection:{self.connection}")
            if len(self._deque) < self._deque.maxlen * 0.9:
                self._deque.append(msg)
                await asyncio.sleep(1)
            else:
                logging.error(f"send msd error:{e}, drop msg:{msg}")

    def send(self, msg: str) -> NoReturn:
        try:
            # if queue full, auto del last value(queue[-1])
            self._deque.appendleft(msg)
        except Exception as e:
            logging.error(f"aiostatsd put:{msg} to queue error:{e}")


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

    def send_graphite(self, key: str, value: int = 0, timestamp: int = time.time(), interval: int = 10) -> NoReturn:
        """interval: Multiple clients timestamp interval synchronization"""
        timestamp: int = int(timestamp) // interval * interval
        msg: str = "{} {} {}".format(key, value, timestamp)
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
        self._sample_rate: Union[float, int] = sample_rate

    def send_statsd(self, statsd_protocol: "StatsdProtocol", sample_rate: Union[int, float, None] = None) -> NoReturn:
        msg: str = statsd_protocol.msg
        sample_rate: Union[float, int] = sample_rate or self._sample_rate
        if "\n" in msg and sample_rate:
            logging.warning("Multi-Metric not support sample rate")
        else:
            if random() > sample_rate:
                msg += f"|@{sample_rate}"
            elif sample_rate > 1:
                logging.warning("sample rate must > 0 & < 1")
                return
        self.send(msg)

    def counter(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> NoReturn:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().counter(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    def timer(self, key: str, value: Union[int, float], sample_rate: Union[int, float, None] = None) -> NoReturn:
        if len(str(value)) == 10:
            value = value * 1000
        value: int = int(value)
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().timer(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    def gauge(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> NoReturn:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().gauge(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    def sets(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> NoReturn:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().sets(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    @contextmanager
    def timeit(self, key: str, sample_rate: Union[int, float, None] = None) -> Iterator[None]:
        """
        Context manager for easily timing methods.
        """
        _loop: "asyncio.AbstractEventLoop" = get_event_loop()
        started_at: float = _loop.time()
        yield
        value: float = _loop.time() - started_at
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

    def send_dog_statsd(
        self, dog_statsd_protocol: "DogStatsdProtocol", sample_rate: Union[int, float, None] = None
    ) -> NoReturn:
        for msg in dog_statsd_protocol.get_msg_list():
            sample_rate: Union[int, float] = sample_rate or self._sample_rate
            if sample_rate != 1 and random() > sample_rate:
                msg += f"|@{sample_rate}"
            self.send(msg)

    def gauge(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().gauge(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def increment(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().increment(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def decrement(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> NoReturn:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().increment(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def timer(
        self,
        key: str,
        value: Union[int, float],
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> NoReturn:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().timer(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    @contextmanager
    def timeit(self, key: str, sample_rate: Union[int, float, None] = None) -> Iterator[None]:
        """
        Context manager for easily timing methods.
        """
        _loop: "asyncio.AbstractEventLoop" = get_event_loop()
        started_at: float = _loop.time()
        yield
        value: float = _loop.time() - started_at
        self.timer(key, value, sample_rate)

    def histogram(
        self,
        key: str,
        value: Union[int, float],
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> NoReturn:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().histogram(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def distribution(
        self,
        key: str,
        value: Union[int, float],
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> NoReturn:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().distribution(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)
