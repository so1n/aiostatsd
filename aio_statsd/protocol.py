#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = "so1n"
__date__ = "2020-02"
from typing import List, Optional, Union

from aio_statsd.utlis import NUM_TYPE


class StatsdProtocol(object):
    """learn more statsd protocol
    https://github.com/statsd/statsd/blob/master/docs/metric_types.md
    """

    def __init__(self, prefix: Optional[str] = None, mtu_limit: int = 1432):
        self._prefix: Optional[str] = prefix
        self.msg: str = ""
        self._mtu_limit: int = mtu_limit

    def _msg_handle(self, msg: str) -> "StatsdProtocol":
        if self._prefix:
            msg = self._prefix + "." + msg
        if self.msg == "":
            self.msg = msg
        else:
            self.msg += "\n" + msg
        if len(self.msg) > self._mtu_limit:
            raise RuntimeError(
                f"msg:{msg} length:{len(self.msg)} > mtu max length limit:{self._mtu_limit}"
                f"learn more:https://github.com/statsd/statsd/blob/master/docs/metric_types.md"
            )
        return self

    def counter(self, key: str, value: NUM_TYPE) -> "StatsdProtocol":
        return self._msg_handle(f"{key}:{value}|c")

    def timer(self, key: str, value: NUM_TYPE) -> "StatsdProtocol":
        return self._msg_handle(f"{key}:{value}|ms")

    def gauge(self, key: str, value: NUM_TYPE) -> "StatsdProtocol":
        return self._msg_handle(f"{key}:{value}|g")

    def increment(self, key: str, value: NUM_TYPE) -> "StatsdProtocol":
        return self._msg_handle(f"{key}:+{value}|g")

    def decrement(self, key: str, value: NUM_TYPE) -> "StatsdProtocol":
        return self._msg_handle(f"{key}:-{value}|g")

    def sets(self, key: str, value: NUM_TYPE) -> "StatsdProtocol":
        return self._msg_handle(f"{key}:{value}|s")


class DogStatsdProtocol(object):
    """https://docs.datadoghq.com/developers/metrics/types/"""

    def __init__(self, prefix: Optional[str] = None):
        self._prefix: Optional[str] = prefix
        self._msg: str = ""
        self._cache: List[str] = []

    def get_msg_list(self) -> List[str]:
        return self._cache

    def build_msg(self, key: str, value: NUM_TYPE, type_: str, tag_dict: Optional[dict] = None) -> "DogStatsdProtocol":
        tag_str: str = "|#" + ",".join(f"{k}:{v}" for k, v in tag_dict.items()) if tag_dict else ""
        msg: str = self._msg + f"{key}:{value}|{type_}" + tag_str
        if self._prefix:
            msg = self._prefix + "." + msg
        self._cache.append(msg)
        return self

    def gauge(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "DogStatsdProtocol":
        return self.build_msg(key, value, "g", tag_dict)

    def increment(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "DogStatsdProtocol":
        return self.build_msg(key, value, "c", tag_dict)

    def decrement(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "DogStatsdProtocol":
        return self.build_msg(key, -value, "c", tag_dict)

    def timer(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "DogStatsdProtocol":
        return self.build_msg(key, value, "ms", tag_dict)

    def histogram(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "DogStatsdProtocol":
        return self.build_msg(key, value, "h", tag_dict)

    def distribution(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "DogStatsdProtocol":
        return self.build_msg(key, value, "d", tag_dict)

    def set(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "DogStatsdProtocol":
        return self.build_msg(key, value, "s", tag_dict)


class TelegrafStatsdProtocol(object):
    """https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd"""

    def __init__(self, prefix: Optional[str] = None):
        self._prefix: Optional[str] = prefix
        self._msg: str = ""
        self._cache: List[str] = []

    def get_msg_list(self) -> List[str]:
        return self._cache

    def build_msg(
        self, key: str, value: Union[int, float, str], type_: str, tag_dict: Optional[dict] = None
    ) -> "TelegrafStatsdProtocol":
        tag_str: str = "," + ",".join(f"{k}={v}" for k, v in tag_dict.items()) if tag_dict else ""
        msg: str = self._msg + f"{key}{tag_str}:{value}|{type_}"
        if self._prefix:
            msg = self._prefix + "." + msg
        self._cache.append(msg)
        return self

    def counter(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "TelegrafStatsdProtocol":
        return self.build_msg(key, value, "c", tag_dict)

    def gauge(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "TelegrafStatsdProtocol":
        return self.build_msg(key, value, "g", tag_dict)

    def increment(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "TelegrafStatsdProtocol":
        return self.build_msg(key, "+" + str(value), "g", tag_dict)

    def decrement(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "TelegrafStatsdProtocol":
        return self.build_msg(key, -value, "g", tag_dict)

    def timer(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "TelegrafStatsdProtocol":
        return self.build_msg(key, value, "ms", tag_dict)

    def histogram(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "TelegrafStatsdProtocol":
        return self.build_msg(key, value, "h", tag_dict)

    def distribution(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "TelegrafStatsdProtocol":
        return self.build_msg(key, value, "d", tag_dict)

    def set(self, key: str, value: NUM_TYPE, tag_dict: Optional[dict] = None) -> "TelegrafStatsdProtocol":
        return self.build_msg(key, value, "s", tag_dict)
