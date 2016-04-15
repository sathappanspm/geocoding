#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from .dbManager import SQLiteWrapper
import pandas as pd
#import ipdb
from . import GeoPoint
from . import loc_default

class BaseGazetteer(object):
    """
    Base Gazetteer class
    """
    def __init__(self):
        pass

    def query(self, name, absoluteMatch=False):
        """
        search for name
        """
        pass


class GeoNames(BaseGazetteer):
    def __init__(self, dbpath, tables=None, priority=None):
        self.db = SQLiteWrapper(dbpath)
        if tables is None:
            self.tables = ('allCities', 'allAdmins', 'allCountries')
        else:
            self.tables = tables

        if priority is None:
            # TODO: Define default priority
            self.priority = None
        else:
            self.priority = priority

    def query(self, name, min_popln=0):
        """
        Search the locations DB for the given name
        params:
        name - string
        absoluteMatch - boolean. If False, pick the longest substring that returns a
                        match

        return:
        list of possible locations the input string refers to
        """
        #res = self.db.query(u"SELECT * FROM geonames_fullText WHERE name='{0}' OR admin1='{0}' or country='{0}'".format(name))
        #result[t] = self.score([dict(r) for r in res for i in r])

        if name in loc_default:
            name = loc_default[name]
        country = self._querycountry(name)
        if country == []:
            admin = self._querystate(name)
            city = self._querycity(name, min_popln=min_popln)
            alternateNames = self._query_alternatenames(name, min_popln)
        else:
            admin = []
            city = []
            alternateNames = []
        #res = self.score(city, admin, country)
        ldist = (city + country + admin + alternateNames)
        #if city == [] and country == [] and admin == []:
            #ipdb.set_trace()
            ## For examples like new york city's times square
        if ldist == [] and "'" in name:
            ldist = self._query_alternatenames(name.split("'", 1)[0])

        if ldist != []:
            df = pd.DataFrame([i.__dict__ for i in ldist])
            df.drop_duplicates('geonameid', inplace=True)
            if df.shape[0] == 1:
                df['confidence'] = 1.0
            else:
                df['confidence'] = 0.5

            try:
                df['population'] = (df['population'] + 1).astype(float)
            except:
                ipdb.set_trace()
            if any(df["ltype"] == "city"):
                dfn = df[df["ltype"] == "city"]
                df.loc[ df['ltype'] == 'city', 'confidence'] += (dfn['population']) / (2 * (dfn['population'].sum()))

            ldist = [GeoPoint(**d) for d in df.to_dict(orient='records')]

        return ldist

    def _querycountry(self, name):
        """
        Check if name is a country name
        """
        return self.db.query(u"""SELECT *, 'country' as 'ltype'
                             FROM allcountries as a INNER JOIN alternatenames as b on a.geonameid=b.geonameid WHERE
                             (country="{0}" OR b.alternatename="{0}") LIMIT 1""".format(name))

    def _querystate(self, name):
        """
        Check if name is an admin name
        """
        stmt = u"""SELECT a.geonameid, a.name as 'admin1', b.country as 'country',
                   'admin' as 'ltype', 'featureCOde' as 'ADM1'
                   FROM allcountries as b INNER JOIN alladmins as a on
                   substr(a.key, 0, 3)=b.ISO
                   WHERE (a.name="{0}" or a.asciiname="{0}")
                   """.format(name)
        return self.db.query(stmt)

    def _querycity(self, name, min_popln=0):
        """
        Check if name is a city name
        """
        stmt = u"""SELECT a.id as geonameid, a.name,
               a.population,a.latitude, a.longitude, c.country as 'country',
               b.name as 'admin1',
               a.featureClass, a.countryCode as 'countryCode', a.featureCOde
               FROM allcountries as c
               INNER JOIN allcities as a ON a.countryCode=c.ISO
               INNER JOIN alladmins as b ON a.countryCode||'.'||a.admin1 = b.key
               WHERE
               (a.name="{0}" or a.asciiname="{0}") and a.population >= {1}
               """.format(name, min_popln)
        res = self.db.query(stmt)
        #if res:
        #    df = pd.DataFrame([i.__dict__ for i in res])
        #    if any(df["ltype"] == "city") and any(df["ltype"] != "city"):
        #        df = df[df["ltype"] == "city"]

        #    df['population'] = (df['population'] + 1).astype(float)
        #    df['confidence'] = (df['population']) / (2 * (df['population'].sum())) + 0.5

        #    res = [GeoPoint(**d) for d in df.to_dict(orient='records')]

        return res

    def _query_alternatenames(self, name, min_popln=0):
        """
        check if name matches alternate name
        """
        stmt = u"""SELECT DISTINCT a.id as geonameid, a.name, a.population,
                a.latitude, a.longitude, c.country as country, b.name as admin1,
                a.featureClass, a.featureCOde, a.countryCode
                FROM alternatenames as d
                INNER JOIN allcities as a ON a.id=d.geonameId
                INNER JOIN allcountries as c ON a.countryCode=c.ISO
                INNER JOIN alladmins as b ON a.countryCode||'.'||a.admin1 = b.key
                WHERE alternatename="{0}" and
                a.population >= {1}""".format(name, min_popln)

        res = self.db.query(stmt)
        #if res:
        #    df = pd.DataFrame([i.__dict__ for i in res])
        #    #if any(df["ltype"] == "city"):
        #    #    df = df[df["ltype"] == "city"]
        #    df['population'] = (df['population'] + 1).astype(float)
        #    df['confidence'] = (df['population']) / (2 * (df['population'].sum())) + 0.5

        #    res = [GeoPoint(**d) for d in df.to_dict(orient='records')]

        return res

    def get_locInfo(self, country=None, admin=None, city=None, strict=False):
        """
        return full loc tuple of name, admin1 name, country, population,
        longitude etc.
        """
        if city and city.lower() == "ciudad de mexico" or city.lower() == u"ciudad de méxico":
            city = "mexico city"

        if strict:
            stmt = u"""SELECT a.id as geonameid, a.name,
                   a.population,a.latitude, a.longitude, c.country as 'country',
                   b.name as 'admin1', a.featureClass,
                   a.featureCOde, a.countryCode as 'countryCode'
                   FROM allcities a
                   INNER JOIN alladmins b ON a.countryCode||"."||a.admin1 = b.key
                   INNER JOIN allcountries c ON a.countryCode=c.ISO
                   WHERE
                   (a.name="{0}" or a.asciiname="{0}") and
                   b.name="{1}"
                   and c.country="{2}" ORDER BY a.population DESC""".format(city, admin, country)
        else:
            stmt = u"""SELECT DISTINCT a.id as geonameid, a.name, a.population,
                    a.latitude, a.longitude, c.country as country, b.name as admin1,
                    a.featureClass, a.featureCOde, a.countryCode
                    FROM allcities as a
                    INNER JOIN alladmins as b ON a.countryCode||"."||a.admin1 = b.key
                    INNER JOIN allcountries as c ON a.countryCode=c.ISO
                    WHERE (a.name="{0}" or a.asciiname="{0}")
                    and
                    c.country="{1}" ORDER BY a.population DESC""".format(city, country)

        res = self.db.query(stmt)
        if res == []:
            res = self._get_locInfo_from_alternate(country=country, admin=admin, city=city)

        return res

    def _get_locInfo_from_alternate(self, country=None, admin=None, city=None):
        stmt = u"""SELECT DISTINCT a.id as geonameid, a.name,
               a.population,a.latitude, a.longitude, c.country as 'country',
               b.name as 'admin1', a.featureClass,
               a.featureCOde, a.countryCode as 'countryCode'
               FROM alternatenames as d
               INNER JOIN allcities a ON a.id=d.geonameId
               INNER JOIN alladmins b ON a.countryCode||"."||a.admin1 = b.key
               INNER JOIN allcountries c ON a.countryCode=c.ISO
               WHERE
               alternatename="{0}"
               and b.name="{1}"
               and c.country="{2}" ORDER BY a.population DESC""".format(city, admin, country)
        return self.db.query(stmt)

    def get_locById(self, locId):
        stmt = u"""SELECT a.id as geonameid, a.name,
               a.population,a.latitude, a.longitude, c.country as 'country',
               b.name as 'admin1',
               a.featureCOde, a.featureClass, a.countryCode as 'countryCode'
               FROM allcities a
               INNER JOIN alladmins b ON a.countryCode||'.'||a.admin1 = b.key
               INNER JOIN allcountries c ON a.countryCode=c.ISO
               WHERE
               a.id='{}'
               """.format(locId)

        return self.db.query(stmt)


    def get_country(self, cc2):
        res = self.db.query("SELECT *, 'country' as 'ltype' FROM allcountries where ISO='{}'".format(cc2))
        for l in res:
            l.confidence = 0.50

        return res
