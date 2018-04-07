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
from .loc_config import loc_default, blacklist
from collections import defaultdict
import logging
import json
import numpy as np

log = logging.getLogger("__init__")
FEATURE_MAP = {"A": "region", "H": "water body",
               "L": "area", "R": "road", "S": "Building",
               "T": "mountain", "U": "undersea", "V": "forest",
               "P": "city", "": "Unknown"}


def safe_convert(obj, ctype, default=None):
    if isempty(obj):
        return default
    try:
        return ctype(obj)
    except Exception as e:
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


def decode(s):
    try:
        return s.decode("utf-8")
    except:
        return s


def geodistance(pt1, pt2):
    # assumes input format is [(lon, lat)..]
    pts_1 = np.radians(np.array(pt1 if isinstance(pt1, list) else [pt1]))
    pts_2 = np.radians(np.array(pt2 if isinstance(pt2, list) else [pt2]))
    dlon = pts_1[:, 0][:, np.newaxis] - pts_2[:, 0]
    dlat = pts_1[:, 1][:, np.newaxis] - pts_2[:, 1]

    dist = (np.sin(dlat / 2.0) ** 2 + np.cos(pts_1[:, 1])[:, np.newaxis] *
            np.cos(pts_2[:, 1]) * np.sin(dlon / 2.0) ** 2)
    dist = 2 * np.arcsin(np.sqrt(dist))
    km = dist * 6367
    return km


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
            self.code2name = json.load(inf)

        data = {l['country'].lower(): l for l in self.code2name.values()}
        data['palestine'] = data['palestinian territory']
        data['united states of america'] = data['united states']
        super(COUNTRY_INFO, self).__init__(data)

    def query(self, name):
        if name.lower() in self:
            return [self[name.lower()]]
        else:
            return []

    def fromISO(self, code):
        code = code.upper()
        if code in self.code2name:
            return self.code2name[code]

        return None


class ADMIN2_INFO(dict):
    def __init__(self, path=None):
        if path is None:
            path = os.path.join(os.path.dirname(__file__),
                                "Admin2Info.json")
        with open(path) as inf:
            data = json.load(inf)

        super(ADMIN2_INFO, self).__init__(data)

    def query(self, country, state, district):
        name = u"{}.{}.{}".format(country, state, district).upper()
        if name in self:
            return self[name]['name']
        else:
            return ""

    def _encode_name(self, country, state, district):
        """
        returns unicode string of format country.state.district
        """
        try:
            name = u"{}.{}.{}".format(country, state, district)
        except UnicodeDecodeError:
            name = u"{}.{}.{}".format(decode(country), decode(state),
                                      decode(district))
        return name


class ADMIN_INFO(dict):
    def __init__(self, path=None):
        if path is None:
            path = os.path.join(os.path.dirname(__file__),
                                "adminInfo.json")

        with open(path) as inf:
            code2name = json.load(inf)
            data = {val['name'].lower(): val for val in code2name.values()}
            data.update({val['asciiname'].lower(): val for val in code2name.values()})

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

        return self.code2name['{}.{}'.format(country, code).upper()]


class GeoPoint(GeoData):
    """
    Location Data Type
    """
    def __init__(self, population=0, **kwargs):
        # self.orig_dict = kwargs
        if isinstance(population, basestring) and population != '':
            self.population = int(population)
        else:
            self.population = population

        if 'geonameid' not in kwargs:
            kwargs['geonameid'] = kwargs.pop('id')

        ltype = self._get_ltype(kwargs)

        self.ltype = ltype
        self.city = ""
        self._score = kwargs.pop("_score", 1.0)
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

        if ltype == 'city':
            self.city = kwargs['name']
        elif ltype not in ('admin1', 'country') and self.admin2:
            self.city = Admin2DB.query(self.countryCode, self.admin1, self.admin2)
        if "admin2" in kwargs:
            self.admin2 = Admin2DB.query(self.countryCode, self.admin1, self.admin2)

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
        return u"/".join([self.country, decode(self.admin1),
                          decode(self.city)])

    def _get_ltype(self, info):

        ltype = None
        if 'ltype' in info:
            return info['ltype']

        fcode = info.get('featureCOde', info.get("featureCode", '')).upper()
        fclass = info.get('featureClass', 'P').upper()

        if fcode == "ADM1":
            ltype = 'admin1'

        elif fclass == 'A' and fcode:
            if fcode[0] not in ('A', 'L'):
                ltype = 'country'
                self.country = info['name']

            elif fcode[:3] == "ADM":  # in ("ADM2", "ADM3", 'ADM4'):
                ltype = 'city'

        if ltype is None:
            ltype = FEATURE_MAP[fclass]

        return ltype

    def _get_country(self):
        if hasattr(self, 'countryCode') and self.countryCode:
            return CountryDB.fromISO(self.countryCode)['country']
        else:
            return ""

    def _get_admin1(self):
        if hasattr(self, 'admin1'):
            try:
                return AdminDB.fromCode(self.countryCode, self.admin1)['admin1']
            except Exception:
                pass
            return self.admin1
        else:
            raise Exception('No admin code')


class LocationDistribution(GeoData):
    def __init__(self, LocObj):
        self.country = defaultdict(int)
        self.admin1 = defaultdict(int)
        self.city = defaultdict(int)
        self.realizations = {}
        if not isinstance(LocObj, list):  # and len(LocObj) == 1:
            LocObj = [LocObj]

        for l in LocObj:
            try:
                if 'asciiname' in l.__dict__ and l.asciiname == "earth":
                    continue

                if l.__dict__.get('featureClass', '') == "l" and l.population > 100000000:
                    continue
            except:
                pass

            pvalue = l.confidence
            lstr = "/".join([l.country, l.admin1, (getattr(l, "admin2", "") or l.city)])
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

        if len(self.realizations) == 1:
            lobj = (self.realizations.values())[0]
            lobj.confidence = lobj._score
            cstr = "/".join([lobj.country, "", ""])
            adminstr = "/".join([lobj.country, lobj.admin1, ""])
            self.country[cstr] = max(self.country[cstr], 0.49)
            self.admin1[adminstr] = max(self.admin1[adminstr], .7)

    def __nonzero__(self):
        return self.realizations != {}

    def isEmpty(self):
        return (not self.__nonzero__())

    def __eq__(self, cmpObj):
        return self.realizations == cmpObj.realizations


CountryDB = COUNTRY_INFO()
AdminDB = ADMIN_INFO()
Admin2DB = ADMIN2_INFO()
