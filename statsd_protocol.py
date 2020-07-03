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

    def __or__(self, other: str):
        if self.msg == '':
            self.msg = other
        self.msg += '\n' + other

    def counter(self, key: str, value: int) -> 'StatsdProtocol':
        self.msg = f'{key}:{value}|c'
        return self

    def timer(self, key: str, value: int) -> 'StatsdProtocol':
        self.msg = f'{key}:{value}|ms'
        return self

    def gauge(self, key: str, value: str) -> 'StatsdProtocol':
        self.msg = f'{key}:{value}|g'
        return self

    def sets(self, key: str, value: int) -> 'StatsdProtocol':
        self.msg = f'{key}:{value}|s'
        return self
