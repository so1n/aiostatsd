import asyncio
import sys

from typing import Callable

__all__ = ["get_event_loop"]


def get_event_loop() -> Callable[[], asyncio.AbstractEventLoop]:
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop

    return asyncio.get_event_loop
