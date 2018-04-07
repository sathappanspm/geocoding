#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""
from gevent import monkey
monkey.patch_all()

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from geocode import BaseGeo
from geoutils.gazetteer import GeoNames
from gevent.pool import Pool as gPool
from multiprocessing.pool import ThreadPool, Pool as mPool
import time
from geoutils import LocationDistribution

strt = time.clock()
gn = GeoNames(dbpath="./geoutils/GN_dump_20160407.sql")
end = time.clock()
print "Init time:{}".format(end-strt)


loclist = ["estados Unidos", "India", "washington", "Madrid", "Paris", "hilton", "chennai", "rio", "madras"]

strt = time.clock()
s = [gn.query(l, min_popln=0) for l in loclist]
end = time.clock()
print "Normal serial processing: {}".format(end-strt)

exit(0)
gp = gPool(10)
print "gpool created"
tp = ThreadPool(10)
print "tpool created"
#mp = (10)
#print "mpool created"
def t(l):
    return gn.query(l, min_popln=0)

##strt = time.clock()
##s = mp.map(t, loclist)
##end = time.clock()
##print "multiprocessing pool: {}".format(end-strt)


strt = time.clock()
s = tp.map(t, loclist)
end = time.clock()
print "Threadpool processing: {}".format(end-strt)

strt = time.clock()
s = gp.map(t, loclist)
end = time.clock()
print "gevent pool processing: {}".format(end-strt)

strt = time.clock()
s = [LocationDistribution(l) for l in s]
end = time.clock()
print "LD time:{}".format(end-strt)
