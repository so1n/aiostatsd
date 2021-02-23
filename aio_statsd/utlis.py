import asyncio
import sys
from typing import Union

__all__ = ["get_event_loop", "NUM_TYPE"]
NUM_TYPE = Union[float, int]


def get_event_loop() -> asyncio.AbstractEventLoop:
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop()

    return asyncio.get_event_loop()
