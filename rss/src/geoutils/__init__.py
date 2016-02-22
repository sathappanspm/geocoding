#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"


import ipdb

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
    def __init__(self, country="", admin1="",
                 latitude=None, longitude=None, population=0,
                 ltype=None, **kwargs):

        self.country = country #or kwargs.get("countryCode", "")
        self.admin1 = admin1
        self.latitude = latitude
        self.longitude = longitude
        if isinstance(population, basestring):
            self.population = 0
        else:
            self.population = population

        self.ltype = ltype
        self.city = ''
        if ltype is None:
            if 'featureCOde' not in kwargs:
                ipdb.set_trace()

            if kwargs['featureCOde'] == 'ADM1':
                self.ltype = 'admin'

            if 'featureClass' in kwargs:
                self.ltype = FEATURE_MAP[kwargs['featureClass']]
                if self.ltype == 'city':
                    self.city = kwargs['name']

        # set all remaining extra information in kwargs
        for arg in kwargs:
            setattr(self, arg, kwargs[arg])

        #if len(self.country) == 2:
        #    ipdb.set_trace()

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
            self.admin1["/".join([LocObj.country, LocObj.admin1])] = 1.0
            self.city[LocObj.__str__()] = 1.0
            self.realizations[LocObj.__str__()] = LocObj
        else:
            for l in LocObj:
                if l.ltype != 'city':
                    pvalue = 0.5
                else:
                    if not hasattr(l, 'confidence'):
                        ipdb.set_trace()
                    pvalue = l.confidence

                    if l.__str__() in self.city:
                        continue
                    #    raise Exception("Duplicate value-{}-{}".format(l.__str__(), l.featureClass))
                    self.city[l.__str__()] = pvalue

                if l.__str__() in self.realizations:
                    continue

                if l.country not in self.country:
                    self.country[l.country] = []

                adminstr = "/".join([l.country, l.admin1, ""])
                if adminstr not in self.admin1:
                    self.admin1[adminstr] = []

                self.country[l.country].append(pvalue)
                self.admin1["/".join([l.country, l.admin1, ""])].append(pvalue)
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
