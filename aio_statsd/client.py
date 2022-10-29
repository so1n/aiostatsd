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
from typing import Any, Deque, Dict, Iterator, Optional, Union

from aio_statsd.connection import Connection
from aio_statsd.protocol import DogStatsdProtocol, StatsdProtocol, TelegrafStatsdProtocol
from aio_statsd.transport_layer_protocol import ProtocolFlag
from aio_statsd.utlis import NUM_TYPE, get_event_loop


logger: logging.Logger = logging.getLogger(__name__)


class Client:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8125,
        protocol: ProtocolFlag = ProtocolFlag.udp,
        timeout: int = 0,
        debug: bool = False,
        close_timeout: int = 9,
        create_timeout: int = 9,
        max_len: int = 1000,
    ) -> None:
        self._queue_empty: "object" = object()
        self._max_len: int = max_len
        self._deque: Deque[str] = deque(maxlen=self._max_len)
        self._listen_future: asyncio.Future = asyncio.Future()
        self._listen_future.set_result(True)

        self._close_timeout: int = close_timeout

        self.connection: Connection = Connection(
            host=host,
            port=port,
            protocol_flag=protocol,
            debug=debug,
            timeout=timeout,
            create_timeout=create_timeout,
        )

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def connect(self) -> None:
        if not self.is_closed:
            raise ConnectionError(f"{self.__class__.__name__} already connected")
        await self.connection.connect()
        self._deque = deque(maxlen=self._max_len)
        self._listen_future = asyncio.ensure_future(self._listen())
        logger.info(f"create {self.__class__.__name__}")

    @property
    def is_closed(self) -> bool:
        return self._listen_future.done()

    async def close(self) -> None:
        if not self.connection.is_closed:
            async def wait_send_msg() -> None:
                while len(self._deque) > 0:
                    await asyncio.sleep(0.1)
            try:
                await asyncio.wait_for(wait_send_msg(), timeout=self._close_timeout)
            except asyncio.TimeoutError:
                pass
            await self.connection.await_close()

        self._deque.clear()
        if not self._listen_future.cancelled():
            self._listen_future.cancel()
        try:
            await self._listen_future
        except asyncio.CancelledError:
            pass

    def _get_by_queue(self) -> Optional[str]:
        try:
            if self._deque:
                return self._deque.pop()
        except IndexError:
            pass
        return None

    async def _listen(self) -> None:
        try:
            while not self.connection.is_closed:
                value: Optional[str] = self._get_by_queue()
                if value:
                    await self._real_send(value)
                else:
                    await asyncio.sleep(0.05)

                if self.connection.future.done():
                    await self.connection.future
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception(f"status:{self.is_closed} error: {e}")

    async def _real_send(self, msg: str) -> None:
        try:
            self.connection.sendto(msg)
        except Exception as e:
            logger.error(f"connection:{self.connection}")
            if len(self._deque) < (self._deque.maxlen or 0) * 0.9:
                self._deque.append(msg)
                await asyncio.sleep(1)
            else:
                logger.error(f"send msd error:{e}, drop msg:{msg}")

    def send(self, msg: str) -> None:
        if self.is_closed:
            raise ConnectionError(f"{self.__class__.__name__} already close.")
        try:
            # if queue full, auto del last value(queue[-1])
            self._deque.appendleft(msg)
        except Exception as e:
            logger.error(f"put:{msg} to queue error:{e}")


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
    ) -> None:
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

    def send_graphite(self, key: str, value: int = 0, timestamp: Optional[int] = None, interval: int = 10) -> None:
        """interval: Multiple clients timestamp interval synchronization"""
        if not timestamp:
            timestamp = int(time.time())
        timestamp = int(timestamp) // interval * interval
        msg: str = "{} {} {}".format(key, value, timestamp)
        self.send(msg)


class TelegrafClient(Client):
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
        user_server_time: bool = False,
        enable_replace_special: bool = False,
    ) -> None:
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
        self._user_server_time: bool = user_server_time
        self._enable_replace_special: bool = enable_replace_special

    @staticmethod
    def _replace_field_value(value: Any) -> str:
        value_type = type(value)
        if value_type is int:
            return str(value) + "i"
        elif value_type is str:
            return f'"{value}"'
        else:
            return str(value)

    @staticmethod
    def _replace_special_str(value: str) -> str:
        return value.replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=").replace('"', "\\")

    def send_telegraf(
        self,
        key: str,
        field_dict: Dict[str, Any],
        tag_dict: Optional[Dict[str, str]] = None,
        user_server_time: Optional[bool] = None,
        enable_replace_special: Optional[bool] = None,
    ) -> None:
        if enable_replace_special is None:
            enable_replace_special = self._enable_replace_special
        if user_server_time is None:
            user_server_time = self._user_server_time

        tag_str: str = ""
        if tag_dict:
            if {"_field", "_measurement", "time"} & set(tag_dict.keys()):
                raise RuntimeError(
                    "Avoid using the reserved keys _field, _measurement, and time."
                    " If reserved keys are included as a tag or field key, the associated point is discarded."
                )
            if enable_replace_special:
                tag_str = "," + ",".join(
                    [
                        f"{self._replace_special_str(key)}={self._replace_special_str(value)}"
                        for key, value in tag_dict.items()
                    ]
                )
            else:
                tag_str = "," + ",".join([f"{key}={value}" for key, value in tag_dict.items()])

        if enable_replace_special:
            field_str: str = " " + ",".join(
                [
                    f"{self._replace_special_str(key)}={self._replace_special_str(self._replace_field_value(value))}"
                    for key, value in field_dict.items()
                ]
            )
        else:
            field_str = " " + ",".join(
                [f"{key}={self._replace_field_value(value)}" for key, value in field_dict.items()]
            )

        msg: str = f"{key}{tag_str}{field_str}"

        if not user_server_time:
            msg += f" {int(time.time() * 1000 * 1000 * 1000)}"
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
    ) -> None:
        if protocol == ProtocolFlag.tcp:
            raise ValueError(f"{self.__class__.__name__} not support {protocol.value} protocol")
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

    def send_statsd(self, statsd_protocol: "StatsdProtocol", sample_rate: Union[int, float, None] = None) -> None:
        msg: str = statsd_protocol.msg
        sample_rate = sample_rate or self._sample_rate
        if "\n" in msg and sample_rate:
            logger.warning("Multi-Metric not support sample rate")

        if sample_rate > 1:
            logger.warning("sample rate must > 0 & < 1")
            return

        if sample_rate != 1 and random() > sample_rate:
            msg += f"|@{sample_rate}"
        elif sample_rate != 1:
            return
        self.send(msg)

    def counter(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> None:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().counter(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    def timer(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> None:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().timer(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    def gauge(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> None:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().gauge(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    def sets(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> None:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().sets(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    def increment(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> None:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().increment(key, value)
        self.send_statsd(statsd_protocol, sample_rate)

    def decrement(self, key: str, value: int, sample_rate: Union[int, float, None] = None) -> None:
        statsd_protocol: "StatsdProtocol" = StatsdProtocol().decrement(key, value)
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
        self.timer(key, int(value * 1000), sample_rate)


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
    ) -> None:
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
    ) -> None:
        for msg in dog_statsd_protocol.get_msg_list():
            sample_rate = sample_rate or self._sample_rate
            if sample_rate != 1 and random() > sample_rate:
                msg += f"|@{sample_rate}"
            elif sample_rate != 1:
                return
            self.send(msg)

    def gauge(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> None:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().gauge(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def increment(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> None:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().increment(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def decrement(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> None:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().decrement(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def timer(
        self,
        key: str,
        value: int,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
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
        self.timer(key, int(value * 1000), sample_rate)

    def histogram(
        self,
        key: str,
        value: NUM_TYPE,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().histogram(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def distribution(
        self,
        key: str,
        value: NUM_TYPE,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().distribution(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)

    def set(
        self,
        key: str,
        value: NUM_TYPE,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
        protocol: "DogStatsdProtocol" = DogStatsdProtocol().set(key, value, tag_dict)
        self.send_dog_statsd(protocol, sample_rate)


class TelegrafStatsdClient(Client):
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
    ) -> None:
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

    def send_telegraf_statsd(
        self, telegraf_statsd_protocol: "TelegrafStatsdProtocol", sample_rate: Union[int, float, None] = None
    ) -> None:
        for msg in telegraf_statsd_protocol.get_msg_list():
            sample_rate = sample_rate or self._sample_rate
            if sample_rate != 1 and random() > sample_rate:
                msg += f"|@{sample_rate}"
            elif sample_rate != 1:
                return
            self.send(msg)

    def gauge(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> None:
        protocol: "TelegrafStatsdProtocol" = TelegrafStatsdProtocol().gauge(key, value, tag_dict)
        self.send_telegraf_statsd(protocol, sample_rate)

    def increment(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> None:
        protocol: "TelegrafStatsdProtocol" = TelegrafStatsdProtocol().increment(key, value, tag_dict)
        self.send_telegraf_statsd(protocol, sample_rate)

    def decrement(
        self, key: str, value: int, sample_rate: Union[int, float, None] = None, tag_dict: Optional[dict] = None
    ) -> None:
        protocol: "TelegrafStatsdProtocol" = TelegrafStatsdProtocol().decrement(key, value, tag_dict)
        self.send_telegraf_statsd(protocol, sample_rate)

    def timer(
        self,
        key: str,
        value: NUM_TYPE,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
        protocol: "TelegrafStatsdProtocol" = TelegrafStatsdProtocol().timer(key, value, tag_dict)
        self.send_telegraf_statsd(protocol, sample_rate)

    @contextmanager
    def timeit(self, key: str, sample_rate: Union[int, float, None] = None) -> Iterator[None]:
        """
        Context manager for easily timing methods.
        """
        _loop: "asyncio.AbstractEventLoop" = get_event_loop()
        started_at: float = _loop.time()
        yield
        value: float = _loop.time() - started_at
        self.timer(key, int(value * 1000), sample_rate)

    def histogram(
        self,
        key: str,
        value: NUM_TYPE,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
        protocol: "TelegrafStatsdProtocol" = TelegrafStatsdProtocol().histogram(key, value, tag_dict)
        self.send_telegraf_statsd(protocol, sample_rate)

    def distribution(
        self,
        key: str,
        value: NUM_TYPE,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
        protocol: "TelegrafStatsdProtocol" = TelegrafStatsdProtocol().distribution(key, value, tag_dict)
        self.send_telegraf_statsd(protocol, sample_rate)

    def set(
        self,
        key: str,
        value: NUM_TYPE,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
        protocol: "TelegrafStatsdProtocol" = TelegrafStatsdProtocol().set(key, value, tag_dict)
        self.send_telegraf_statsd(protocol, sample_rate)

    def counter(
        self,
        key: str,
        value: NUM_TYPE,
        sample_rate: Union[int, float, None] = None,
        tag_dict: Optional[dict] = None,
    ) -> None:
        protocol: "TelegrafStatsdProtocol" = TelegrafStatsdProtocol().counter(key, value, tag_dict)
        self.send_telegraf_statsd(protocol, sample_rate)
