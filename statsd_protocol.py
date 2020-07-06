#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""learn more statsd protocol
https://github.com/statsd/statsd/blob/master/docs/metric_types.md
"""
__author__ = 'so1n'
__date__ = '2020-02'


class StatsdProtocol(object):

    def __init__(self):
        self.msg = ''

    def _msg_handle(self, msg: str):
        if self.msg == '':
            self.msg = msg
        self.msg += '\n' + msg

    def counter(self, key: str, value: int) -> 'StatsdProtocol':
        self._msg_handle(f'{key}:{value}|c')
        return self

    def timer(self, key: str, value: int) -> 'StatsdProtocol':
        self._msg_handle(f'{key}:{value}|ms')
        return self

    def gauge(self, key: str, value: str) -> 'StatsdProtocol':
        self._msg_handle(f'{key}:{value}|g')
        return self

    def sets(self, key: str, value: int) -> 'StatsdProtocol':
        self._msg_handle(f'{key}:{value}|s')
        return self
