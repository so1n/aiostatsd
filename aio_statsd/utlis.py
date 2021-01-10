import asyncio
import sys

__all__ = ["get_event_loop"]


def get_event_loop() -> asyncio.AbstractEventLoop:
    if sys.version_info >= (3, 7):
        return asyncio.get_running_loop()

    return asyncio.get_event_loop()
