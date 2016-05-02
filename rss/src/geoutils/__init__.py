#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"


import os
import sys
from loc_config import loc_default

FEATURE_MAP = {"A": "region", "H": "water body",
               "L": "parks area", "R": "road", "S": "Building",
               "T": "mountain", "U": "undersea", "V": "forest",
               "P": "city"}


def safe_convert(obj, ctype, default=None):
    if isempty(obj):
        return default
    try:
        return ctype(obj)
    except Exception, e:
        raise e


def isempty(s):
    """
    return if input object(string) is empty
    """
    if s in (None, "", "-"):
        return True

    return False


def encode(s):
    try:
        return s.encode("utf-8")
    except:
        return s


# code from smart_open package https://github.com/piskvorky/smart_open

def make_closing(base, **attrs):
    """
    Add support for `with Base(attrs) as fout:` to the base class if it's missing.
    The base class' `close()` method will be called on context exit, to always close
    the file properly. This is needed for gzip.GzipFile, bz2.BZ2File etc in older
    Pythons (<=2.6), which otherwise
    raise "AttributeError: GzipFile instance has no attribute '__exit__'".
    """
    if not hasattr(base, '__enter__'):
        attrs['__enter__'] = lambda self: self
    if not hasattr(base, '__exit__'):
        attrs['__exit__'] = lambda self, type, value, traceback: self.close()
    return type('Closing' + base.__name__, (base, object), attrs)


# code from smart_open package https://github.com/piskvorky/smart_open
def smart_open(fname, mode='rb'):
    """
    Stream from/to local filesystem, transparently (de)compressing gzip and bz2
    files if necessary.
    """
    _, ext = os.path.splitext(fname)

    if ext == '.bz2':
        PY2 = sys.version_info[0] == 2
        if PY2:
            from bz2file import BZ2File
        else:
            from bz2 import BZ2File
        return make_closing(BZ2File)(fname, mode)

    if ext == '.gz':
        from gzip import GzipFile
        return make_closing(GzipFile)(fname, mode)

    return open(fname, mode)


class GeoData(object):
    def __init__(self):
        pass


class GeoPoint(GeoData):
    """
    Location Data Type
    """
    def __init__(self, population=0,
                 ltype=None, **kwargs):

        # self.orig_dict = kwargs
        if isinstance(population, basestring):
            self.population = 0
        else:
            self.population = population

        self.city = ''
        if ltype is None:
            if kwargs.get('featureCOde', "") == 'ADM1':
                ltype = 'admin'

            if 'featureClass' in kwargs:
                ltype = FEATURE_MAP[kwargs['featureClass']]
                if ltype == 'city':
                    self.city = kwargs['name']

        self.ltype = ltype
        # set all remaining extra information in kwargs
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

        if hasattr(self, 'admin1'):
            if self.admin1 is None:
                self.admin1 = ''
        else:
            self.admin1 = ''

    def to_dict(self):
        return self.__dict__

    def __str__(self):
        """
        print country/admin/city string
        """
        return encode("/".join([self.country, self.admin1, self.city]))


class LocationDistribution(GeoData):
    def __init__(self, LocObj):
        self.country = {}
        self.admin1 = {}
        self.city = {}
        self.realizations = {}
        if isinstance(LocObj, list) and len(LocObj) == 1:
            LocObj = LocObj[0]

        if not isinstance(LocObj, list):
            self.country[LocObj.country] = 1.0
            self.admin1["/".join([LocObj.country, LocObj.admin1, ""])] = 1.0
            self.city[LocObj.__str__()] = 1.0
            self.realizations[LocObj.__str__()] = LocObj
        else:
            for l in LocObj:
                if l.__str__() in self.realizations:
                    continue

                if l.ltype != 'city':
                    pvalue = 0.5
                else:
                    pvalue = l.confidence

                    # if l.__str__() in self.city:
                    #     continue
                    #    raise Exception("Duplicate
                    # value-{}-{}".format(l.__str__(), l.featureClass))
                    self.city[l.__str__()] = pvalue

                if l.country not in self.country:
                    self.country[l.country] = []

                adminstr = "/".join([l.country, l.admin1, ""])
                if adminstr not in self.admin1:
                    self.admin1[adminstr] = []

                self.country[l.country].append(pvalue)
                self.admin1[adminstr].append(pvalue)
                self.realizations[l.__str__()] = l

            for co in self.country:
                # p^2 d^2
                self.country[co] = 0.49 * max(self.country[co]) ** 2

            for ad in self.admin1:
                self.admin1[ad] = 0.7 * max(self.admin1[ad]) ** 2

    def __nonzero__(self):
        return self.realizations != {}

    def isEmpty(self):
        return (not self.__nonzero__())

    def __eq__(self, cmpObj):
        return self.realizations == cmpObj.realizations
