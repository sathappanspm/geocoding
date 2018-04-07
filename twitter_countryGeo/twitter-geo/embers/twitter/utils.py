#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""TagLogger caches the tag counts for a while before sending them to the db
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

from collections import OrderedDict, defaultdict


class TagLogger(object):
    """TagLogger caches the tag counts for a while before sending them
    to the db
    """
    def __init__(self, tag_db, maxn=10):
        self.db = tag_db
        self.cache = OrderedDict()
        self.batch = self.db.batch()
        self.maxn = maxn

    def __enter__(self):
        self.cache = OrderedDict()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        while len(self.cache) > 0:
            self.pop()
        self.batch.send()

    def pop(self, n=1):
        for i in range(n):
            t, tag_q = self.cache.popitem(last=False)
            for tag, v in tag_q.items():
                self.db.insert(tag, v, _time=t, batch=self.batch)

    def count_tags(self, time, tags):
        k = self.key(time)
        if k not in self.cache:
            self.cache[k] = defaultdict(int)
            if len(self.cache) > self.maxn:
                self.pop()
        for tag in tags:
            if isinstance(tag, unicode):
                tag = tag.encode("utf8")
            self.cache[k][tag] += 1

    def key(self, t):
        return t.replace(second=0)
