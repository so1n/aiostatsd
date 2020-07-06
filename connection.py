#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-02'
import asyncio
import logging

from typing import Optional, NoReturn
from transport_layer_protocol import DatagramProtocol, TcpProtocol, Protocol

logger = logging.getLogger()


class Connection(object):
    def __init__(
            self,
            host: str,
            port: int,
            protocol_flag: Protocol,
            debug: bool = False,
            timeout: int = 0,
            create_timeout: int = 5,
            close_timeout: int = 5,
            loop: Optional['asyncio.get_event_loop'] = None
    ):
        self._is_closing: bool = False
        self._is_close: bool = True
        self._debug: bool = debug
        self._protocol_flag: Protocol = protocol_flag
        self._close_timeout: int = close_timeout
        self._create_timeout: int = create_timeout
        self._loop = loop if loop else asyncio.get_event_loop()

        self._connection_info = f'{protocol_flag}://{host}:{port}'
        if protocol_flag == Protocol.udp:
            self._protocol: DatagramProtocol = DatagramProtocol(timeout=timeout)
            self._connection = self._loop.create_datagram_endpoint(
                lambda: self._protocol,
                remote_addr=(host, port)
            )
        elif protocol_flag == Protocol.tcp:
            self._protocol: TcpProtocol = TcpProtocol(timeout=timeout)
            self._connection = self._loop.create_connection(
                lambda: self._protocol,
                host=host, port=port
            )
        else:
            raise ConnectionError(f'Not support protocol:{protocol_flag}')

        if debug:
            self.sendto = self._sendto_debug
        else:
            self.sendto = self._sendto

    async def connect(self) -> NoReturn:
        try:
            await asyncio.wait_for(self._connection, timeout=self._create_timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError(f'create connection:{self._connection_info} timeout') from e
        logging.debug(f'create connection:{self._connection_info}')
        self._is_close = False

    def _sendto(self, data: str) -> NoReturn:
        if not self._is_closing:
            self._protocol.send(bytes(data, encoding="utf8"))

    def _sendto_debug(self, data: str) -> NoReturn:
        if not self._is_closing:
            try:
                logging.debug(f'send msg:{data}')
                self._protocol.send(bytes(data, encoding="utf8"))
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
        logging.debug(f'close connection: {self._connection_info}')
