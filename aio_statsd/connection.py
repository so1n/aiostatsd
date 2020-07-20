#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-02'
import asyncio
import logging

from typing import NoReturn
from aio_statsd.transport_layer_protocol import DatagramProtocol, TcpProtocol, ProtocolFlag
from aio_statsd.utlis import get_event_loop


class Connection(object):
    def __init__(
            self,
            host: str,
            port: int,
            protocol_flag: ProtocolFlag,
            debug: bool,
            timeout: int,
            create_timeout: int,
    ):
        self._debug: bool = debug
        self._protocol_flag: ProtocolFlag = protocol_flag
        self._create_timeout: int = create_timeout

        self._connection_info = f'{protocol_flag}://{host}:{port}'
        _loop = get_event_loop()
        if protocol_flag == ProtocolFlag.udp:
            self._connection: DatagramProtocol = DatagramProtocol(timeout=timeout)
            self._connection_proxy = _loop.create_datagram_endpoint(
                lambda: self._connection,
                remote_addr=(host, port)
            )
        elif protocol_flag == ProtocolFlag.tcp:
            self._connection: TcpProtocol = TcpProtocol(timeout=timeout)
            self._connection_proxy = _loop.create_connection(
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
        if not self.is_closed:
            self._connection.send(bytes(data, encoding="utf8"))

    def _sendto_debug(self, data: str) -> NoReturn:
        if not self.is_closed:
            logging.debug(f'send msg:{data}')
            self._connection.send(bytes(data, encoding="utf8"))

    @property
    def is_closed(self) -> bool:
        return self._connection.is_closed

    def close(self):
        self._connection.close()

    async def wait_closed(self):
        await self._connection.wait_closed()

    async def await_close(self):
        await self._connection.await_close()
        logging.debug(f'close connection: {self._connection_info}')
