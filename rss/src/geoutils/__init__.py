#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.2"


import os
import sys
from loc_config import loc_default, blacklist
from collections import defaultdict
import logging
import json
import ipdb

log = logging.getLogger("__init__")
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


class COUNTRY_INFO(dict):
    def __init__(self, path=None):
        if path is None:
            path = os.path.join(os.path.dirname(__file__),
                                "countryInfo.json")
        with open(path) as inf:
            data = json.load(inf)

        self.code2name = {l['ISO']: l for l in data.viewvalues()}
        super(COUNTRY_INFO, self).__init__(data)

    def query(self, name):
        if name in self:
            return [self[name]]
        else:
            return []

    def fromISO(self, code):
        return self.code2name[code]


class ADMIN_INFO(dict):
    def __init__(self, path=None):
        if path is None:
            path = os.path.join(os.path.dirname(__file__),
                                "adminInfo.json")

        with open(path) as inf:
            dd = [json.loads(l) for l in inf]
            data = {}
            code2name = {}
            for l in dd:
                l['ltype'] = 'admin1'
                data[l['admin1'].lower()] = l
                data[l['admin1_ascii'].lower()] = l
                data[l['asciiname'].lower()] = l
                code2name[l['key']] = l

        self.code2name = code2name
        super(ADMIN_INFO, self).__init__(data)

    def query(self, name):
        if name in self:
            admin = self[name]
            admin['countryCode'] = admin['key'].split('.')[0]
            return [admin]
        else:
            return []

    def fromCode(self, country, code):
        if code == "00":
            return {'admin1': ""}

        return self.code2name['{}.{}'.format(country, code)]


class GeoPoint(GeoData):
    """
    Location Data Type
    """
    def __init__(self, population=0, **kwargs):

        # self.orig_dict = kwargs
        if isinstance(population, basestring):
            self.population = 0
        else:
            self.population = population

        #self.city, self.admin1, self.country = '', '', ''
        ltype = self._get_ltype(kwargs)

        if ltype == 'city':
            self.city = kwargs['name']

        self.ltype = ltype
        # log.info("{}-ltype-{}".format(encode(self.city), kwargs.get('featureCOde', '')))
        assert kwargs.get('featureClass', 'A') in ("P", "A")
        # set all remaining extra information in kwargs
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

        #self.admin1 = '' if self.admin1 is None else self.admin1
        self.country = self._get_country()
        if ltype != 'country':
            self.admin1 = self._get_admin1()
            if ltype == 'admin1':
                self.city = ""
        else:
            self.admin1, self.city = "", ""

    def to_dict(self):
        return self.__dict__

    def __str__(self):
        """
        print country/admin/city string
        """
        return "/".join([self.country, self.admin1, self.city])

    def _get_ltype(self, info):

        ltype = None
        if 'ltype' in info:
            return info['ltype']

        fcode = info.get('featureCOde', "")
        fclass = info.get('featureClass', 'P')

        if fcode == "ADM1":
            ltype = 'admin1'

        if fclass == 'A' and fcode:
            if fcode[0] not in ('A', 'L'):
                ltype = 'country'

            elif fcode in ("ADM2", "ADM3"):
                ltype = 'city'

        if ltype is None:
            ltype = FEATURE_MAP[fclass]

        return ltype

    def _get_country(self):
        if self.ltype == 'country':
            return self.country

        if hasattr(self, 'countryCode'):
            return CountryDB.fromISO(self.countryCode)['country']
        else:
            ipdb.set_trace()
            raise Exception("No Country info found")

    def _get_admin1(self):
        if self.ltype == 'admin1':
            return self.admin1

        if hasattr(self, 'admin1'):
            if self.admin1.isupper() or self.admin1.isdigit():
                try:
                    return AdminDB.fromCode(self.countryCode, self.admin1)['admin1']
                except Exception:
                    log.exception('no admin for {}.{}'.format(self.countryCode, self.admin1))
                    self.admin1 = ""
            return self.admin1
        else:
            ipdb.set_trace()
            raise Exception('No admin code')


class LocationDistribution(GeoData):
    def __init__(self, LocObj):
        self.country = defaultdict(int)
        self.admin1 = defaultdict(int)
        self.city = defaultdict(int)
        self.realizations = {}
        if not isinstance(LocObj, list): #and len(LocObj) == 1:
            LocObj = [LocObj]

        #if not isinstance(LocObj, list):
        #    self.country["/".join([LocObj.country, "", ""])] = 1.0

        #    if LocObj.admin1:
        #        self.admin1["/".join([LocObj.country, LocObj.admin1, ""])] = 1.0
        #        if LocObj.city:
        #            self.city[LocObj.__str__()] = 1.0

        #    self.realizations[LocObj.__str__()] = LocObj
        #else:
        for l in LocObj:
            pvalue = l.confidence
            lstr = l.__str__()
            if lstr not in self.realizations:
                self.realizations[lstr] = l
            else:
                if self.realizations[lstr].confidence < pvalue:
                    self.realizations[lstr] = l

            cstr = "/".join([l.country, "", ""])
            adminstr = "/".join([l.country, l.admin1, ""])
            if l.ltype == "country":
                self.country[cstr] = max(self.country[cstr], pvalue)

            elif l.ltype == 'admin1':
                self.country[cstr] = max(self.country[cstr],
                                         0.7 * (pvalue ** 2))
                self.admin1[adminstr] = max(self.admin1[adminstr], pvalue)

            else:
                self.city[lstr] = max(self.city[lstr], pvalue)
                self.admin1[adminstr] = max(self.admin1[adminstr],
                                            0.7 * (pvalue ** 2))
                self.country[cstr] = max(self.country[cstr],
                                         0.49 * (pvalue ** 2))
                #if l.__str__() in self.realizations:
                #    if l.ltype == 'city' and self.city[l.__str__()] < l.confidence
                #    continue

                #if l.ltype != 'city':
                #    pvalue = 0.5
                #else:

                    # if l.__str__() in self.city:
                    #     continue
                    #    raise Exception("Duplicate
                    # value-{}-{}".format(l.__str__(), l.featureClass))

                #if l.ltype == 'city':
                #    self.city[l.__str__()] = pvalue

                #if l.country not in self.country:
                #    self.country[l.country] = []

                #adminstr = "/".join([l.country, l.admin1, ""])
                #if adminstr not in self.admin1:
                #    self.admin1[adminstr] = []

                #self.country[l.country].append(pvalue)
                #self.admin1[adminstr].append(pvalue)

        #if len(self.realizations) == 1:
        #    sfunc = lambda w, x: x
        #else:
        #    sfunc = lambda w, x: w * (x ** 2)

        #self.country = {co: sfunc(0.49, val) for co, val in self.country.viewitems()}
        #self.admin1 = {ad: sfunc(0.7, val) for ad, val in self.admin1.viewitems()}
        #for co in self.country:
            # p^2 d^2
            #self.country[co] = 0.49 * max(self.country[co]) ** 2

        #for ad in self.admin1:
        #    self.admin1[ad] = 0.7 * self.admin1[ad] ** 2

    def __nonzero__(self):
        return self.realizations != {}

    def isEmpty(self):
        return (not self.__nonzero__())

    def __eq__(self, cmpObj):
        return self.realizations == cmpObj.realizations


CountryDB = COUNTRY_INFO()
AdminDB = ADMIN_INFO()
