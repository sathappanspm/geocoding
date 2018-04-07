#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'
__version__ = '0.0.1'



from ..utils import *
#from nose import tools
import pytz
import uuid
import time

timestr = "Tue, 12 Mar 2013 12:00:00 +0000"
#jjjtimetup = .struct_time(tm_year=2013, tm_mon=3,
#                           tm_mday=12, tm_hour=12,
#                           tm_min=0, tm_sec=0,
#                           tm_wday=1, tm_yday=71, tm_isdst=0)


def test_twittime():
    dt = twittime_to_dt(timestr)
    #assert dt.timetuple() == timetup
    assert dt.tzinfo == pytz.UTC


def test_now():
    dt = now()
    assert dt.tzinfo == pytz.UTC


def uuid_to_dt():
    dt = uuid_to_dt(uuid.uuid1())
    assert dt.tzinfo == pytz.UTC
