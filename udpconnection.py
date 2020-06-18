#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-02'
import asyncio
import logging

from asyncio import DatagramProtocol, DatagramTransport
from typing import Optional, NoReturn

logger = logging.getLogger()


class UdpConnection(object):
    def __init__(
            self, host: str, port: int,
            debug: bool = False,
            close_timeout: Optional[float] = None,
    ) -> NoReturn:
        self._host = host
        self._port = port
        self._is_closing: bool = False
        self._is_close: bool = True
        self._debug = debug
        self._protocol: _DatagramProtocol = _DatagramProtocol()
        self._close_timeout = close_timeout
        if debug:
            self.sendto = self._sendto_debug
        else:
            self.sendto = self._sendto

    async def connect(self) -> NoReturn:
        _loop = asyncio.get_event_loop()
        await _loop.create_datagram_endpoint(
            lambda: self._protocol,
            remote_addr=(self._host, self._port)
        )
        logging.debug(f'create conn: {self._host}:{self._port}')
        self._is_close = False

    def _sendto(self, data: str) -> NoReturn:
        if not self._is_closing:
            self._protocol.sendto(bytes(data, encoding="utf8"))

    def _sendto_debug(self, data: str) -> NoReturn:
        if not self._is_closing:
            try:
                logging.debug(f'send msg:{data}')
                self._protocol.sendto(bytes(data, encoding="utf8"))
            except Exception as e:
                logging.error(f'send error. msg:{data} error:{e}')

    def is_close(self) -> bool:
        return self._is_close

    async def close(self) -> NoReturn:
        self._is_closing = True
        try:
            await asyncio.wait_for(self._close(), timeout=self._close_timeout)
            self._is_close = True
        except asyncio.TimeoutError as e:
            self._is_closing = False
            raise e

    async def _close(self) -> NoReturn:
        await self._protocol.close()
        logging.debug(f'close conn: {self._host}:{self._port}')


class _DatagramProtocol(DatagramProtocol):

    def __init__(self) -> None:
        self._transport: Optional[DatagramTransport] = None
        self._closed: Optional[asyncio.Future] = None

    async def close(self) -> None:
        if self._transport is None:
            return

        self._transport.close()
        await self._closed

    def connection_made(self, transport):
        self._transport = transport
        self._closed = asyncio.Future()

    def connection_lost(self, _exc):
        self._transport = None
        self._closed.set_result(True)

    def sendto(self, data: bytes) -> None:
        if self._transport is None:
            raise ConnectionError('connection is close')
        self._transport.sendto(data)
