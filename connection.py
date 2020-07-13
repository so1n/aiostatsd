#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-02'
import asyncio
import logging

from typing import NoReturn
from transport_layer_protocol import DatagramProtocol, TcpProtocol, ProtocolFlag


class Connection(object):
    def __init__(
            self,
            host: str,
            port: int,
            protocol_flag: ProtocolFlag,
            debug: bool,
            timeout: int,
            create_timeout: int,
            loop: 'asyncio.get_event_loop'
    ):
        self._debug: bool = debug
        self._protocol_flag: ProtocolFlag = protocol_flag
        self._create_timeout: int = create_timeout
        self._loop = loop

        self._connection_info = f'{protocol_flag}://{host}:{port}'
        if protocol_flag == ProtocolFlag.udp:
            self._connection: DatagramProtocol = DatagramProtocol(timeout=timeout)
            self._connection_proxy = self._loop.create_datagram_endpoint(
                lambda: self._connection,
                remote_addr=(host, port)
            )
        elif protocol_flag == ProtocolFlag.tcp:
            self._connection: TcpProtocol = TcpProtocol(timeout=timeout)
            self._connection_proxy = self._loop.create_connection(
                lambda: self._connection,
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
            await asyncio.wait_for(self._connection_proxy, timeout=self._create_timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError(f'create connection:{self._connection_info} timeout') from e
        logging.debug(f'create connection:{self._connection_info}')

    def _sendto(self, data: str) -> NoReturn:
        if not self.is_closed():
            self._connection.send(bytes(data, encoding="utf8"))

    def _sendto_debug(self, data: str) -> NoReturn:
        if not self.is_closed():
            logging.debug(f'send msg:{data}')
            self._connection.send(bytes(data, encoding="utf8"))

    def is_closed(self) -> bool:
        return self._connection.is_closed()

    async def close(self) -> NoReturn:
        await self._connection.close()
        logging.debug(f'close connection: {self._connection_info}')
