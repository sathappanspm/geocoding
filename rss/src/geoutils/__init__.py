#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"


FEATURE_MAP = {"A": "region", "H": "water body",
               "L": "parks area", "R": "road", "S": "Building",
               "T": "mountain", "U": "undersea", "V": "forest",
               "P": "city"}


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


class GeoData(object):
    def __init__(self):
        pass


class GeoPoint(GeoData):
    """
    Location Data Type
    """
    def __init__(self, country="", admin1="", city="",
                 latitude=None, longitude=None, population=0,
                 ltype=None, **kwargs):

        self.country = country or kwargs.get("countryCode", "")
        self.admin1 = admin1
        self.city = city
        self.latitude = latitude
        self.longitude = longitude
        self.population = population
        self.ltype = ltype
        if "name" in kwargs:
            if ltype == "admin":
                self.country, self.admin1 = kwargs['key'].split(".")
            elif ltype == "city":
                self.city = kwargs["name"]

            del(kwargs['name'])

        if ltype == "country" or "ISO" in kwargs:
            self.country = kwargs['ISO']
            del(kwargs['ISO'])

        if 'featureClass' in kwargs:
            self.ltype = FEATURE_MAP[kwargs['featureClass']]

        # set all remaining extra information in kwargs
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

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
        self.baseObj = {}
        if isinstance(LocObj, list) and len(LocObj) == 1:
            LocObj = LocObj[0]

        if not isinstance(LocObj, list):
            self.country[LocObj.country] = 1.0
            self.admin1["/".join([LocObj.country, LocObj.admin1])] = 1.0
            self.city[LocObj.__str__()] = 1.0
            self.baseObj[LocObj.__str__()] = LocObj
        else:
            for l in LocObj:
                if l.ltype != 'city':
                    pvalue = 0.51
                else:
                    pvalue = l.confidence

                    if l.__str__() in self.city:
                        continue
                    #    raise Exception("Duplicate value-{}-{}".format(l.__str__(), l.featureClass))
                    self.city[l.__str__()] = pvalue

                if l.__str__() in self.baseObj:
                    continue

                if l.country not in self.country:
                    self.country[l.country] = []

                adminstr = "/".join([l.country, l.admin1, ""])
                if adminstr not in self.admin1:
                    self.admin1[adminstr] = []

                self.country[l.country].append(pvalue)
                self.admin1["/".join([l.country, l.admin1, ""])].append(pvalue)
                self.baseObj[l.__str__()] = l

            for co in self.country:
                self.country[co] = max(self.country[co]) ** 2

            for ad in self.admin1:
                self.admin1[ad] = max(self.admin1[ad]) ** 2

    def __nonzero__(self):
        return self.baseObj != {}

    def __eq__(self, cmpObj):
        return self.baseObj == cmpObj.baseObj
