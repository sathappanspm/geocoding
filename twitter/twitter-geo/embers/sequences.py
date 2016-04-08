#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

import time
import struct
import os
import atexit
import weakref

import logging

logger = logging.getLogger("solidpubsub")

datasize = struct.calcsize("LL")
MAGIC = 2 ** 2 ** 2 ** 1 + 3912983


class SequenceRecorder(object):
    """This records sequence numbers in a file. It does
    so by using an unsigned long format.  It is not guaranteed perfect
    but should give you the correct sequence number within the blocksize

    Though in most case, Python will catch the error and garbage collect
    causing the file to be flushed to disk.
    """
    def __init__(self, fname, blocksize=4096, clear=False, runid=None):
        self._fname = fname
        self._blocksize = blocksize
        if not clear:
            mode = "a"
            self.seq, self.runid = self.read()
        else:
            mode = "w"
            self.seq = 0
#             if runid is None:
#                 raise Exception("Please choose a runid for any new files")
            self.runid = runid

        if self.runid is None:
            self.runid = "\0" * 16
        self.lastw = time.time()
        self._f = open(fname, mode)
        self._f.seek(0)
        self._f.write(self.runid)
        self.flush()
        self.lastw = time.time()
        atexit.register(SequenceRecorder.finish, weakref.ref(self))

    def read(self):
        if not os.path.exists(self._fname):
            return 0, None
        mdata = 0
        f = open(self._fname, "r")
        runid = f.read(16)

        if len(runid) < 16:
            return 0, None
        bad_count = 0
        fdata = f.read(datasize)
        while len(fdata) == datasize:
            i, chksum = struct.unpack("LL", fdata)
            if i ^ MAGIC != chksum:
                fdata = f.read(datasize)
                logger.info("skipping bad i in sequence file %d" % i)
                bad_count = bad_count + 1
                if bad_count < 100:
                    continue
                else:
                    break
            mdata = max(mdata, i)
            fdata = f.read(datasize)

        return mdata, runid

    def write(self, n, runid):
        self.seq = n
        now = time.time()
        if n % self._blocksize == 0 or self.runid != runid:
            self.runid = runid
            self._f.seek(0)
            self._f.truncate(0)
            self._f.write(runid)
            self._f.write(struct.pack("LL", n, n ^ MAGIC))
            self._f.truncate(16 + datasize)
            self.flush()
            self.lastw = time.time()
        else:
            self._f.write(struct.pack("LL", n, n ^ MAGIC))

        if now - self.lastw > 1:
            self.flush()
            self.lastw = time.time()

    def flush(self):
        self._f.flush()
        os.fsync(self._f.fileno())

    @staticmethod
    def finish(self):
        self = self()
        if self:
            self.flush()
            self._f.close()


if __name__ == "__main__":
    try:
        sr = SequenceRecorder("outfile.seq")
        print sr.seq
        time.sleep(1)
        for i in range(1, 1000000):
            sr.write(i)
            print i
    except Exception, e:
        print i
        raise
