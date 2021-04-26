#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = "so1n"
__date__ = "2020-02"
import asyncio
import logging
from typing import Union

from aio_statsd.transport_layer_protocol import DatagramProtocol, ProtocolFlag, TcpProtocol
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
        self._host: str = host
        self._port: int = port
        self._connection_info = f"{protocol_flag}://{host}:{port}"
        if protocol_flag == ProtocolFlag.udp:
            self._connection: Union[DatagramProtocol, TcpProtocol] = DatagramProtocol(timeout=timeout)
        elif protocol_flag == ProtocolFlag.tcp:
            self._connection = TcpProtocol(timeout=timeout)
        else:
            raise ConnectionError(f"Not support protocol:{protocol_flag}")

        self.sendto = self._sendto if not debug else self._sendto_debug

    async def connect(self) -> None:
        _loop = get_event_loop()
        if self._protocol_flag == ProtocolFlag.udp:
            connection_proxy = _loop.create_datagram_endpoint(
                lambda: self._connection, remote_addr=(self._host, self._port)
            )
        elif self._protocol_flag == ProtocolFlag.tcp:
            connection_proxy = _loop.create_connection(lambda: self._connection, host=self._host, port=self._port)
        else:
            raise ConnectionError(f"Not support protocol:{self._protocol_flag}")
        try:
            await asyncio.wait_for(connection_proxy, timeout=self._create_timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError(f"create connection:{self._connection_info} timeout") from e
        logging.debug(f"create connection:{self._connection_info}")

    def _sendto(self, data: str) -> None:
        if not self.is_closed:
            self._connection.send(bytes(data, encoding="utf8"))

    def _sendto_debug(self, data: str) -> None:
        if not self.is_closed:
            logging.debug(f"send msg:{data}")
            self._connection.send(bytes(data, encoding="utf8"))

    @property
    def is_closed(self) -> bool:
        return self._connection.is_closed

    @property
    def future(self) -> asyncio.Future:
        return self._connection.future

    def close(self) -> None:
        self._connection.close()

    async def wait_closed(self) -> None:
        await self._connection.wait_closed()

    async def await_close(self) -> None:
        await self._connection.await_close()
        logging.debug(f"close connection: {self._connection_info}")
