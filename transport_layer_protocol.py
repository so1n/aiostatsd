#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-07'
import asyncio
import logging
from enum import Enum

from typing import Optional, Union

__all__ = ['Protocol', 'TcpProtocol', 'DatagramProtocol']
logger = logging.getLogger()


class Protocol(Enum):
    Tcp = 1
    Udp = 2


class _TransportMixin(asyncio.BaseProtocol):

    def __init__(self, timeout: int = 0) -> None:
        self._transport: Union[asyncio.Transport, asyncio.DatagramTransport, None] = None
        self._future: Optional[asyncio.Future] = None

        self._keep_alive_future: Optional[asyncio.Future] = None
        self._timeout = timeout
        if self._timeout > 0:
            self._keep_alive_future: asyncio.Future = asyncio.ensure_future(self._call_later(self._close))

    async def _call_later(self, func):
        """If running in uvicorn, the loop may not be running, can not use loop._call_later"""
        await asyncio.sleep(self._timeout)
        await func()

    def keep_alive(self):
        if self._timeout:
            self._keep_alive_future.cancel()
            self._keep_alive_future = asyncio.ensure_future(self._call_later(self._close))

    async def close(self) -> None:
        self._close()
        await self._future

    def _close(self):
        if self._transport is None:
            return

        if not self._transport.is_closing():
            self._transport.close()
            self._transport = None
            self._future.set_result(True)

    def is_closed(self) -> bool:
        if not self._future:
            return False
        return self._future.done()

    def connection_made(self, transport):
        self._transport = transport
        self._future = asyncio.Future()

    def connection_lost(self, _exc):
        self._close()

    def check_transport(self):
        if self._transport is None:
            raise ConnectionError('connection is close')


class TcpProtocol(asyncio.Protocol, _TransportMixin):

    def data_received(self, data):
        # TODO add data to queue
        logger.debug(f'receive data:{data}')
        self.keep_alive()

    def pause_reading(self):
        self._transport and self._transport.pause_reading()

    def resume_reading(self):
        self._transport and self._transport.resume_reading()

    def eof_received(self):
        self._close()

    def send(self, data: bytes) -> None:
        self.check_transport()
        self._transport.write(data)


class DatagramProtocol(asyncio.DatagramProtocol, _TransportMixin):

    def datagram_received(self, data: bytes, peer_name: str, *arg):
        logger.debug(f'receive data:{data} form:{peer_name}')
        self.keep_alive()

    def error_received(self, exc):
        raise exc

    def send(self, data: bytes) -> None:
        self.check_transport()
        self._transport.sendto(data)
