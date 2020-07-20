#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'so1n'
__date__ = '2020-02'
import logging
from typing import List, Optional


class StatsdProtocol(object):
    """learn more statsd protocol
    https://github.com/statsd/statsd/blob/master/docs/metric_types.md
    """
    def __init__(self, prefix: Optional[str] = None, mtu_limit: int = 1432):
        if prefix:
            self.msg = f'{prefix}.'
        else:
            self.msg = ''
        self._mtu_limit = mtu_limit

    def _msg_handle(self, msg: str):
        if self.msg == '':
            self.msg = msg
        else:
            self.msg += '\n' + msg
        if len(self.msg) > self._mtu_limit:
            logging.warning(
                f'msg length > mtu max length limit:{self._mtu_limit}'
                f'learn more:https://github.com/statsd/statsd/blob/master/docs/metric_types.md'
            )

    def counter(self, key: str, value: int) -> 'StatsdProtocol':
        self._msg_handle(f'{key}:{value}|c')
        return self

    def timer(self, key: str, value: int) -> 'StatsdProtocol':
        self._msg_handle(f'{key}:{value}|ms')
        return self

    def gauge(self, key: str, value: int) -> 'StatsdProtocol':
        self._msg_handle(f'{key}:{value}|g')
        return self

    def sets(self, key: str, value: int) -> 'StatsdProtocol':
        self._msg_handle(f'{key}:{value}|s')
        return self


class DogStatsdProtocol(object):

    def __init_t_(self, prefix: Optional[str] = None):
        if prefix:
            self._msg = f'{prefix}.'
        else:
            self._msg = ''
        self._cache: List[str] = []

    def get_msg_list(self) -> List[str]:
        return self._cache

    @staticmethod
    def build_tag(tag_dict: Optional[dict] = None):
        if not tag_dict:
            return ""
        return "|#" + ",".join(f"{k}:{v}" for k, v in tag_dict.items())

    def gauge(self, key: str, value: int, tag_dict: Optional[dict] = None) -> 'DogStatsdProtocol':
        msg = self._msg + f'{key}:{value}|g' + self.build_tag(tag_dict)
        self._cache.append(msg)
        return self

    def increment(self, key: str, value: int, tag_dict: Optional[dict] = None) -> 'DogStatsdProtocol':
        msg = self._msg + f'{key}:{value}|c' + self.build_tag(tag_dict)
        self._cache.append(msg)
        return self

    def decrement(self, key: str, value: int, tag_dict: Optional[dict] = None) -> 'DogStatsdProtocol':
        msg = self._msg + f'{key}:-{value}|c' + self.build_tag(tag_dict)
        self._cache.append(msg)
        return self

    def timer(self, key: str, value: int, tag_dict: Optional[dict] = None) -> 'DogStatsdProtocol':
        msg = self._msg + f'{key}:{value}|ms' + self.build_tag(tag_dict)
        self._cache.append(msg)
        return self

    def histogram(self, key: str, value: int, tag_dict: Optional[dict] = None) -> 'DogStatsdProtocol':
        msg = self._msg + f'{key}:{value}|h' + self.build_tag(tag_dict)
        self._cache.append(msg)
        return self

    def distribution(self, key: str, value: int, tag_dict: Optional[dict] = None) -> 'DogStatsdProtocol':
        msg = self._msg + f'{key}:{value}|d' + self.build_tag(tag_dict)
        self._cache.append(msg)
        return self
