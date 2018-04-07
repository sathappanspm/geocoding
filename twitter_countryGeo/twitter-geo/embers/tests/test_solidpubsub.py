#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

from ..solidpubsub import Publisher, Subscriber, SolidSocket
from ..solidpubsub import SolidSocketException, DemuxPublisher
import time
import functools
import traceback

import zmq
import multiprocessing


class TestThread(multiprocessing.Process):
    def __init__(self, target, timeout):  # , timeout=0):
        super(TestThread, self).__init__()  # *args, **kwargs)
        self.result = False
        self._target = target
        self.result = multiprocessing.Array('c', " " * 10240)
        self.result.value = ""
        self._timeout = timeout
        self.__name__ = target.__name__

    def run(self):
        try:
            self._target(*self._args, **self._kwargs)
        except Exception:
            #traceback.print_exc()
            v = traceback.format_exc()
            #print len(v)
            self.result.value = str(v)[:10240]
            #print self.result.value
            raise

    def __call__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self.start()
        self.join(timeout=self._timeout)
        if self.is_alive():
            self.terminate()
            raise Exception("Failed to quit in time")
        elif self.result.value == "":
            return
        else:
            raise Exception(self.result.value)


def AsyncTest(timeout=0):
    def dec(target):
        t = TestThread(target, timeout)

        @functools.wraps(target)
        def fun(*args, **kwargs):
            return t(*args, **kwargs)
        return fun
    return dec


class PubbieTester(Publisher):
    def __init__(self, *args, **kwargs):
        self._times = kwargs['times']
        del kwargs['times']
        super(PubbieTester, self).__init__(*args, **kwargs)
        self._i = 0
        time.sleep(0.1)

    def get_data(self):
        i = self._i
        self._i += 1
        print i
        time.sleep(0.1)
        if i >= self._times:
            self.stop()
            return None
        return ('', i)


class SubbieTester(Subscriber):
    def __init__(self, *args, **kwargs):
        super(SubbieTester, self).__init__(*args, **kwargs)
        self.vals = []

    def handle(self, msg):
        print msg
        #self.go = False
        self.vals += [msg]


@AsyncTest(timeout=3)
def test_timeout():
    ctx = zmq.Context()
    s = SolidSocket(ctx, zmq.SUB, default_timeout=1)
    try:
        s.connect("tcp://localhost:12344")
        s.recv()
    except SolidSocketException:
        return
    assert False


@AsyncTest(timeout=3)
def test_solidsocket():
    ctx = zmq.Context()

    pub = PubbieTester(ctx, times=3)
    mpub = DemuxPublisher(ctx)
    sub = SubbieTester(ctx, timeout=.2)  # retry=False)
    pub.start()
    mpub.start()
    sub.start()
    threads = [pub]
    print "here"
    for t in threads:
        t.join()
    v = [i.data for i in sub.vals]
    print sub.vals
    assert v == range(3)


@AsyncTest(timeout=4)
def test_solidsocket_sync():
    ctx = zmq.Context()

    pub = PubbieTester(ctx, times=3)
    mpub = DemuxPublisher(ctx)
    sub = SubbieTester(ctx, timeout=.2)  # , retry=False)
    pub.start()
    mpub.start()
    time.sleep(1)
    sub.start()
    threads = [pub]
    for t in threads:
        t.join()
    time.sleep(1)
    v = [i.data for i in sub.vals]
    print sub.vals
    assert v == range(3)
