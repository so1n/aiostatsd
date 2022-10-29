#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = "so1n"
__date__ = "2020-07"
import asyncio
import logging
from enum import Enum
from typing import Any, Callable, List, NoReturn, Optional, Tuple

__all__ = ["ProtocolFlag", "TcpProtocol", "DatagramProtocol"]
logger = logging.getLogger(__name__)


class ProtocolFlag(Enum):
    tcp = 1
    udp = 2

    def __str__(self) -> str:
        return "%s" % self._name_


class _ProtocolMixin(asyncio.BaseProtocol):
    def __init__(self, timeout: int = 0) -> None:
        self._transport: Optional[asyncio.BaseTransport] = None
        self.future: asyncio.Future = asyncio.Future()
        self.future.set_result(True)

        self._before_event: List[Callable] = []
        self._after_event: List[Callable] = []

        self._keep_alive_future: Optional[asyncio.TimerHandle] = None
        self._timeout = timeout
        self._loop = asyncio.get_event_loop()

        if self._timeout:
            self._before_event.append(self.create_keep_alive)
            self._after_event.append(self.cancel_keep_alive)

    def cancel_keep_alive(self) -> None:
        if self._keep_alive_future:
            self._keep_alive_future.cancel()
            self._keep_alive_future = None

    def create_keep_alive(self) -> None:
        def timeout_handle() -> NoReturn:
            e: Exception = asyncio.TimeoutError(f"No response data received within {self._timeout} seconds")
            if not self.future.done():
                self.future.set_exception(e)
            raise e

        self._keep_alive_future = self._loop.call_later(self._timeout, timeout_handle)

    async def await_close(self) -> None:
        self.close()
        await self.wait_closed()

    async def wait_closed(self) -> None:
        if self.future:
            await self.future
        else:
            await asyncio.sleep(0)

    def close(self) -> None:
        if not self.future.done():
            self.future.set_result(True)
        if self._keep_alive_future and self._keep_alive_future.cancelled():
            self._keep_alive_future.cancel()

        if self._transport is None:
            return
        elif not self._transport.is_closing():
            self._transport.close()
            self._transport = None

    @property
    def is_closed(self) -> bool:
        return self.future.done()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = transport
        if not self.future.done():
            self.future.cancelled()
        self.future = asyncio.Future()

    def connection_lost(self, _exc: Optional[Exception]) -> None:
        if not self.future.done():
            if _exc:
                self.future.set_exception(_exc)
            else:
                self.future.set_result(True)
        if _exc:
            raise ConnectionError from _exc

    def before_transport(self) -> None:
        if self._transport is None:
            raise ConnectionError("connection is close")
        for fn in self._before_event:
            fn()

    def after_transport(self, data: bytes, *args: Any, **kwargs: Any) -> None:
        logger.debug("receive data: %s args: %s, kwargs: %s", data, args, kwargs)
        for fn in self._after_event:
            fn()


class TcpProtocol(asyncio.Protocol, _ProtocolMixin):
    _transport: asyncio.Transport

    def __init__(self, timeout: int = 0, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
        super().__init__(timeout)
        self._drain_waiter: Optional[asyncio.Future] = None
        self._paused: bool = False
        self._connection_lost: bool = False
        self._loop = loop if loop else asyncio.get_event_loop()

    def data_received(self, data: bytes) -> None:
        self.after_transport(data)

    def pause_reading(self) -> None:
        if self._transport:
            self._transport.pause_reading()
        assert not self._paused
        self._paused = True
        if self._loop.get_debug():
            logger.debug("%r pauses writing", self)

    def resume_reading(self) -> None:
        if self._transport:
            self._transport.resume_reading()
        self._paused = False
        if self._loop.get_debug():
            logger.debug("%r resumes writing", self)

        if self._drain_waiter is not None:
            if not self._drain_waiter.done():
                self._drain_waiter.set_result(None)
            self._drain_waiter = None

    def connection_lost(self, exc: Optional[Exception]) -> None:
        try:
            super().connection_lost(exc)
        except ConnectionError:
            pass
        self._connection_lost = True
        # Wake up the writer if currently paused.
        if not self._paused:
            return

        if self._drain_waiter is None:
            return
        if self._drain_waiter.done():
            return
        if exc is None:
            self._drain_waiter.set_result(None)
        else:
            self._drain_waiter.set_exception(exc)
        self._drain_waiter = None

    async def drain(self) -> None:
        if self._connection_lost:
            raise ConnectionResetError("Connection lost")
        if not self._paused:
            return

        assert self._drain_waiter is None or self._drain_waiter.cancelled()
        self._drain_waiter = asyncio.Future()
        await self._drain_waiter

    def eof_received(self) -> None:
        logger.debug(f"{self} receive `EOF` flag")
        self.close()

    def send(self, data: bytes) -> None:
        self.before_transport()
        if self._transport:
            self._transport.write(data)


class DatagramProtocol(asyncio.DatagramProtocol, _ProtocolMixin):
    _transport: asyncio.DatagramTransport

    def datagram_received(self, data: bytes, peer_name: Tuple[str, int]) -> None:
        self.after_transport(data, peer_name)

    def error_received(self, exc: Exception) -> None:
        self.future.set_exception(exc)
        self.close()

    def send(self, data: bytes) -> None:
        self.before_transport()
        if self._transport:
            self._transport.sendto(data)
