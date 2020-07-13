"""

"""
import asyncio
import logging

from collections import deque
from typing import Deque, List, Set, Optional

from connection import Connection
from transport_layer_protocol import ProtocolFlag


class Pool(object):
    """design like aioredis.pool"""
    def __init__(
            self,
            host: str,
            port: int,
            protocol_flag: ProtocolFlag,
            debug: bool,
            timeout: int,
            create_timeout: int,
            loop: 'asyncio.get_event_loop',
            min_size: int = 1,
            max_size: int = 10,
    ):
        self._host: str = host
        self._port: int = port
        self._protocol: ProtocolFlag = protocol_flag
        self._debug: bool = debug
        self._timeout: int = timeout
        self._create_timeout: int = create_timeout
        self._loop = loop if loop else asyncio.get_event_loop()

        self._min_size = min_size
        self._max_size = max_size

        self._is_closed: bool = True
        self._cond = asyncio.Condition()
        self._pool: Deque[Connection] = deque(maxlen=max_size)
        self._used_set: Set[Connection] = set()

    @property
    def free_size(self):
        """Current number of free connections."""
        return len(self._pool)

    @property
    def size(self):
        return self.free_size + self.use_size

    @property
    def use_size(self):
        return len(self._used_set)

    def _drop_closed(self):
        for i in range(self.free_size):
            conn = self._pool[0]
            if conn.is_closed():
                self._pool.popleft()
            else:
                self._pool.rotate(-1)

    def sendto(self, msg: str):
        asyncio.ensure_future(self.send_await(msg))

    async def send_await(self, msg: str):
        conn = await self.acquire()
        try:
            return conn.sendto(msg)
        finally:
            self.release(conn)

    async def close(self):
        waiter_list: List[Connection] = []
        while self._pool:
            waiter_list.append(self._pool.popleft())
        for conn in self._used_set:
            waiter_list.append(conn)
        await asyncio.gather(*waiter_list)

        logging.debug("Closed %d connection(s)", len(waiter_list))

    async def connect(self):
        self._drop_closed()
        while self.size < self._max_size:
            try:
                conn = Connection(
                    self._host,
                    self._port,
                    self._protocol,
                    self._debug,
                    self._timeout,
                    self._create_timeout,
                    self._loop
                )
                await conn.connect()
                self._pool.append(conn)
            finally:
                self._drop_closed()
                await asyncio.sleep(0.01)

    async def acquire(self):
        async with self._cond:
            while True:
                await self.connect()
                if self.free_size:
                    conn = self._pool.popleft()
                    self._used_set.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    def release(self, conn: Connection):
        if conn not in self._used_set:
            raise RuntimeError("connection not in user pool")
        self._used_set.remove(conn)
        if conn.is_closed():
            asyncio.ensure_future(self._wakeup(conn))
        elif self._max_size and self.free_size < self._max_size:
            self._pool.append(conn)
            asyncio.ensure_future(self._wakeup())
        else:
            asyncio.ensure_future(self._wakeup(conn))

    async def _wakeup(self, conn: Optional[Connection] = None):
        async with self._cond:
            self._cond.notify()
        if conn is not None:
            await conn.close()
