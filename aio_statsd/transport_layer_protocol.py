#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-07'
import asyncio
import logging
from enum import Enum

from typing import Optional, Union

__all__ = ['ProtocolFlag', 'TcpProtocol', 'DatagramProtocol']
logger = logging.getLogger()


class ProtocolFlag(Enum):
    tcp = 1
    udp = 2

    def __str__(self):
        return "%s" % self._name_

    def __repr__(self):
        return "%s" % self._name_


class _ProtocolMixin(asyncio.BaseProtocol):

    def __init__(self, timeout: int = 0) -> None:
        self._transport: Union[asyncio.Transport, asyncio.DatagramTransport, None] = None
        self._future: Optional[asyncio.Future] = None

        self._before_event = []
        self._after_event = []

        self._keep_alive_future: Optional[asyncio.Future] = None
        self._timeout = timeout
        self._loop = asyncio.get_event_loop()

        if self._timeout:
            self._before_event.append(self.create_keep_alive)
            self._after_event.append(self.cancel_keep_alive)

    def timeout_handle(self):
        raise asyncio.TimeoutError(f'No response data received within {self._timeout} seconds')

    def cancel_keep_alive(self):
        if self._keep_alive_future:
            self._keep_alive_future.cancel()
            self._keep_alive_future = None

    def create_keep_alive(self):
        self._keep_alive_future = self._loop.call_later(self._timeout, self.timeout_handle)

    async def await_close(self) -> None:
        self.close()
        await self.wait_closed()

    async def wait_closed(self):
        await self._future

    def close(self):
        if self._transport is None:
            return

        if not self._transport.is_closing():
            self._transport.close()
            self._transport = None

    @property
    def is_closed(self) -> bool:
        if not self._future:
            return True
        return self._future.done()

    def connection_made(self, transport):
        self._transport = transport
        self._future = asyncio.Future()

    def connection_lost(self, _exc):
        if self._future.done():
            return
        self._future.set_result(True)
        if _exc:
            raise ConnectionError from _exc

    def before_transport(self):
        if self._transport is None:
            raise ConnectionError('connection is close')
        for fn in self._before_event:
            fn()

    def after_transport(self, data, *args, **kwargs):
        logger.debug(f'receive data:{data} args:{args}, kwargs:{kwargs}')
        for fn in self._after_event:
            fn()


class TcpProtocol(asyncio.Protocol, _ProtocolMixin):

    def __init__(self, timeout: int = 0, loop=None):
        super().__init__(timeout)
        self._drain_waiter: Optional[asyncio.Future] = None
        self._paused: bool = False
        self._connection_lost: bool = False
        self._loop = loop if loop else asyncio.get_event_loop()

    def data_received(self, data):
        self.after_transport(data)

    def pause_reading(self):
        self._transport and self._transport.pause_reading()
        assert not self._paused
        self._paused = True
        if self._loop.get_debug():
            logger.debug("%r pauses writing", self)

    def resume_reading(self):
        self._transport and self._transport.resume_reading()
        self._paused = False
        if self._loop.get_debug():
            logger.debug("%r resumes writing", self)

        if self._drain_waiter is not None:
            if not self._drain_waiter.done():
                self._drain_waiter.set_result(None)
            self._drain_waiter = None

    def connection_lost(self, exc):
        super().connection_lost(exc)
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

    async def drain(self):
        if self._connection_lost:
            raise ConnectionResetError('Connection lost')
        if not self._paused:
            return

        assert self._drain_waiter is None or self._drain_waiter.cancelled()
        self._drain_waiter = asyncio.Future()
        await self._drain_waiter

    def eof_received(self):
        logger.debug(f'{self} receive `EOF` flag')
        self.close()

    def send(self, data: bytes) -> None:
        self.before_transport()
        self._transport.write(data)


class DatagramProtocol(asyncio.DatagramProtocol, _ProtocolMixin):

    def datagram_received(self, data: bytes, peer_name: str, *arg):
        self.after_transport(data, peer_name)

    def error_received(self, exc):
        self.close()
        raise exc

    def send(self, data: bytes) -> None:
        self.before_transport()
        self._transport.sendto(data)
