#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo
"""

import gevent.monkey
#gevent.monkey.patch_all()

import logging
import time
# import gevent
from gevent.lock import BoundedSemaphore
from gevent.pool import Pool
import json

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'
__version__ = '0.0.1'


__processor__ = 'workerpool'
log = logging.getLogger(__processor__)


class WorkerPool(object):
    """Docstring for WorkerPool """

    def __init__(self, input, output, func, nthreads=800):
        """@todo: to be defined

        :param input: @todo
        :param output: @todo
        :param func: @todo
        :param qname: @todo

        """
        self._func = func
        self._input = input
        self._output = output
        self._lock = BoundedSemaphore(1)
        self._pool = Pool(nthreads)
        self._nthreads = nthreads
        self._true = 0
        self._false = 0
        self._nogeo = 0
        self._notruth = 0

    def run_one(self, msg):
        result = self._func(msg)
        if result is not None:
            with self._lock:
                self._output.write((json.dumps(result, ensure_ascii=False)).encode("utf-8") + "\n")
                #if not result['true_geo']:
                #    self._notruth += 1
                #elif ('country' not in result['embersGeoCode']):
                #    self._nogeo += 1
                #elif result['true_geo']['country'].lower() == result['embersGeoCode']['country'].lower():
                #    self._true += 1
                #else:
                #    self._false += 1

    def run(self):
        last = time.time()
        for msg in self._input:
            self._pool.spawn(self.run_one, msg)
            if time.time() - last > 10:
                log.info("Workers running={}".format(self._nthreads - self._pool.free_count()))
                last = time.time()
        self._pool.join()

#    def cleanup_workers(self):
#        dones = [w.done for w in self._workers]
#        for done, w in zip(dones, self._workers):
#            if done:
#                fin_job = w.ret
#                self._output.write(fin_job)
#        self._workers = [w for done, w in zip(dones, self._workers) if not done]
#
    def stop(self):
        self._pool.join()


