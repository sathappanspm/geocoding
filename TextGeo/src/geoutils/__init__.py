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
from six import string_types


log = logging.getLogger("root.geoutils")
FEATURE_MAP = {"A": "administrative region", "H": "water body",
               "L": "area", "R": "road", "S": "Building",
               "T": "mountain", "U": "undersea", "V": "forest",
               "P": "city", "": "Unknown"}
FEATURERANK = {"CON": 1, "PCL": 2, "ADM": 3, "PPLA": 4, "PPL": 5, "PPLC": 3,
               "STLMT": 3, "L": 6, "V": 7, "H": 8, "R": 9, "S": 10}
FEATURE_CLASS_RANK = {"A": 1, "P": 2, "V": 5, "H": 5, "T": 5, "L": 6, "R": 6, "S": 9, "": 10}
FEATURE_WEIGHTS = {'adm1': 0.8, 'adm2': 0.7, 'adm3': 0.6, 'adm4': 0.5, 'ppla2': 0.6, 'ppla': 0.7,
                   'adm5': 0.4, 'pcli': 1.0, 'ppla3': 0.5, 'pplc': 0.3, 'ppl': 0.2, 'cont': 1.0}


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
    if s in (None, "", "-", []):
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

        data = {l['country'].lower(): l for l in list(self.code2name.values())}
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
        name = "{}.{}.{}".format(country, state, district).upper()
        if name in self:
            return self[name]['name']
        else:
            return ""

    def _encode_name(self, country, state, district):
        """
        returns unicode string of format country.state.district
        """
        try:
            name = "{}.{}.{}".format(country, state, district)
        except UnicodeDecodeError:
            name = "{}.{}.{}".format(decode(country), decode(state),
                                      decode(district))
        return name


class ADMIN_INFO(dict):
    def __init__(self, path=None):
        if path is None:
            path = os.path.join(os.path.dirname(__file__),
                                "adminInfo.json")

        with open(path) as inf:
            code2name = json.load(inf)

        self.code2name = code2name
        if 'BH.18' not in self.code2name:
            self.code2name['BH.18'] = {'_score': 0.5789473684210527,
                                       'admin1': 'central governorate',
                                       'admin2': '',
                                       'admin3': '',
                                       'admin4': '',
                                       'alternatenames': ['central governorate',
                                                          'al muhafazah al wusta',
                                                          'al mu\u1e29\u0101faz\u0327ah al wus\u0163\xe1',
                                                          'almhafzt alwsty',
                                                          '\u0627\u0644\u0645\u062d\u0627\u0641\u0638\u0629 \u0627\u0644\u0648\u0633\u0637\u0649'],
                                       'asciiname': 'central governorate',
                                       'cc2': '',
                                       'city': '',
                                       'confidence': 0.5789473684210527,
                                       'coordinates': [50.56667, 26.16667],
                                       'country': 'Bahrain',
                                       'countryCode': 'bh',
                                       'dem': '6',
                                       'elevation': -1,
                                       'featureClass': 'a',
                                       'featureCode': 'adm1h',
                                       'geonameid': '7090973',
                                       'id': '7090973',
                                       'ltype': 'admin1',
                                       'modificationDate': '2017-03-21',
                                       'name': 'central governorate',
                                       'population': 0,
                                       'timezone': 'asia/bahrain',
                                       'key': 'bh.18'}

        data = {val['name'].lower(): val for val in list(code2name.values())}
        data.update({val['asciiname'].lower(): val for val in list(code2name.values())})
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
        if isinstance(population, string_types) and population != '':
            self.population = int(population)
        else:
            self.population = population

        if 'geonameid' not in kwargs:
            kwargs['geonameid'] = kwargs.pop('id')

        ltype = self._get_ltype(kwargs)

        if 'coordinates' in kwargs:
            if isinstance(kwargs['coordinates'], str):
                self.coordinates = [float(_) for _ in kwargs.pop('coordinates').split(',')][::-1]
            else:
                self.coordinates = kwargs.pop('coordinates')

        self.ltype = ltype
        self.city = ""
        self._score = kwargs.pop("_score", 1.0)
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

        if "admin2" in kwargs:
            self.admin2Code = self.admin2
            self.admin2 = Admin2DB.query(self.countryCode, self.admin1, self.admin2)

        if ltype == 'city':
            self.city = kwargs['name']
        elif ltype not in ('admin1', 'country'):
            if self.admin2:
                self.city = Admin2DB.query(self.countryCode, self.admin1, self.admin2)
            else:
                self.city = kwargs['name']

        self.country = self._get_country()
        if ltype != 'country':
            self.admin1Code = self.admin1
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
        return "/".join([self.country, decode(self.admin1),
                          decode(self.city)])

    def _get_ltype(self, info):

        ltype = None
        if 'ltype' in info:
            return info['ltype']

        fcode = info.get('featureCOde', info.get("featureCode", '')).upper()
        fclass = info.get('featureClass', 'P').upper()

        if fcode[:4] == "ADM1":
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
            if self.ltype == 'admin1':
                return self.name
            else:
                return self.admin1
        else:
            raise Exception('No admin code')

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setitem__(self, name, value):
        self.__dict__[name] = value
        return self


class LocationDistribution(GeoData):
    def __init__(self, LocObj):
        self.country = defaultdict(int)
        self.admin1 = defaultdict(int)
        self.city = defaultdict(int)
        self.realizations = {}
        if not isinstance(LocObj, list):  # and len(LocObj) == 1:
            LocObj = [LocObj]

        for l in LocObj:
            ## escape if the obtained place is encompasses the entire earth or any of the continents
            try:
                if 'asciiname' in l.__dict__ and l.asciiname == "earth":
                    continue

                if l.__dict__.get('featureClass', '') == "l" and l.population > 100000000:
                    continue
            except Exception as e:
                log.exception(str(e))
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
            lobj = list(self.realizations.values())[0]
            lobj.confidence = lobj._score
            cstr = "/".join([lobj.country, "", ""])
            adminstr = "/".join([lobj.country, lobj.admin1, ""])
            self.country[cstr] = max(self.country[cstr], 0.49)
            self.admin1[adminstr] = max(self.admin1[adminstr], .7)

    def __bool__(self):
        return self.realizations != {}

    def isempty(self):
        return (not self.__nonzero__())

    def __eq__(self, cmpObj):
        return self.realizations == cmpObj.realizations

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setitem__(self, name, value):
        self.__dict__[name] = value
        return self

CountryDB = COUNTRY_INFO()
AdminDB = ADMIN_INFO()
Admin2DB = ADMIN2_INFO()
