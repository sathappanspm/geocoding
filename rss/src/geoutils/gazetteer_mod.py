#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

# import gevent
import json
from .dbManager import SQLiteWrapper, ESWrapper, MongoDBWrapper
import pandas as pd
from . import GeoPoint, encode, blacklist, loc_default, isempty, CountryDB, AdminDB
# from . import loc_default, blacklist
# from . import isempty
from pylru import lrudecorator
import logging
import os
#import ipdb

log = logging.getLogger("rssgeocoder")


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
    def __init__(self, db, priority=None):
        self.db = db  # SQLiteWrapper(dbpath)

        if priority is None:
            # TODO: Define default priority
            self.priority = None
        else:
            self.priority = priority

        if isinstance(db, ESWrapper):
            self._query = self.esquery
        else:
            self._query = self.sqlquery
        #self.countries = json.load(open(os.path.join(os.path.dirname(__file__),
        #                                             "countryInfo.json")))

    def esquery(self, name, min_popln=0, **kwargs):
        """
        Search the locations DB for the given name
        params:
            name - string
            min_popln - integer
        return:
            list of possible locations the input string refers to
        """
        name = name.strip().lower()
        if name in loc_default:
            name = loc_default[name]

        if name in blacklist:
            return []

        country = self._querycountry(name)
        if country == []:
            admin = self._querystate(name)
            city_alt = self.db.query(name, qtype="combined", query_name="should",
                                     min_popln=min_popln, **kwargs)
            #city = self.db.query(name, **kwargs)
            #alternateNames = self.db.query(name, qtype="relaxed", **kwargs)
        else:
            admin, city_alt = [], []

        ldist = (city_alt + country + admin)
        #return [tmp.__dict__ for tmp in ldist]
        return ldist

    #@lrudecorator(10000)
    #@profile
    def sqlquery(self, name, min_popln=0):
        """
        Search the locations DB for the given name
        params:
            name - string
            min_popln - integer
        return:
            list of possible locations the input string refers to
        """
        name = name.strip().lower()
        if name in loc_default:
            name = loc_default[name]

        if name in blacklist:
            return []

        country = self._querycountry(name)
        #admin = self._querystate(name)
        #city = self._querycity(name, min_popln=min_popln)
        #alternateNames = self._query_alternatenames(name, min_popln)
        if country == []:
            admin = self._querystate(name)
            city = self._querycity(name)
            alternateNames = self._query_alternatenames(name)
            #g1 = gevent.spawn(self._querystate, name)
            #g2 = gevent.spawn(self._querycity, name, min_popln=min_popln)
            #g3 = gevent.spawn(self._query_alternatenames, name, min_popln)
            #gevent.joinall([g1, g2, g3])
            #admin, city, alternateNames = g1.value, g2.value, g3.value
        else:
            admin, city, alternateNames = [], [], []

        ldist = (city + country + admin + alternateNames)
        if ldist == [] and "'" in name:
            log.info('splitting location name on quote mark-{}'.format(encode(name)))
            ldist = self._query_alternatenames(name.split("'", 1)[0])
        #return [t.__dict__ for t in ldist]
        return ldist

    @lrudecorator(1000)
    def query(self, name, min_popln=0, **kwargs):
        ldist = self._query(name, min_popln=min_popln, **kwargs)
        ldist = self._get_loc_confidence(ldist, min_popln)
        return ldist

    def _get_loc_confidence(self, ldist, min_popln):
        if ldist != []:
            ldist = {l.geonameid: l for l in ldist}.values()

        popsum = sum([(float(l.population) + 10.0) for l in ldist])
        rmset = set()
        for idx, l in enumerate(ldist):
            if l.featureClass.lower() == "l":
                rmset.add(idx)

            l.confidence = ((float(l.population) + 10.0)/popsum)  *l._score

        if len(rmset) < len(ldist):
            ldist = [l for idx, l in enumerate(ldist) if idx not in rmset]
            popsum = sum([(float(l.population) + 10.0) for l in ldist])
            for l in ldist:
                l.confidence = ((float(l.population) + 10.0)/popsum) *l._score

        return ldist

    #def _get_loc_confidence(self, ldist, min_popln):
    #    if ldist != []:
    #        df = pd.DataFrame(ldist)
    #        df.drop_duplicates('geonameid', inplace=True)
    #        if df.shape[0] == 1:
    #            df['confidence'] = 1.0
    #        else:
    #            #df['confidence'] = 0.0

    #            #dfcity = df[df.featureClass.str.match('A|P')]
    #            #if not dfcity.empty:
    #            #    df = dfcity.reindex(copy=True)
    #            dfpop = df[df.population > (min_popln + 1)]
    #            if not dfpop.empty:
    #                df = dfpop.reindex(copy=True)
    #                try:
    #                    df['population'] = (df['population'].astype(int) + 10).astype(float)
    #                except Exception, e:
    #                    raise e

    #                #dfn = df[df["ltype"] == "city"]
    #                #if not dfn.empty:
    #                #    df.loc[df['ltype'] == 'city', 'confidence'] += ((dfn['population']) /
    #                #                                                    (2 * (dfn['population'].sum())))
    #                #df['confidence'] += (df['population'] / (2 * df['population'].sum()))
    #                df['confidence'] = (df['population'] / (df['population'].sum()))
    #            else:
    #                df['confidence'] = 0.01

    #        ldist = [GeoPoint(**d) for d in df.to_dict(orient='records')]
    #    return ldist

    def _querycountry(self, name):
        """
        Check if name is a country name
        """
        return [GeoPoint(**l) for l in CountryDB.query(name)]
        #if name in self.countries:
        #    return [GeoPoint(**self.countries[name])]
        #else:
        #    return []
        #return self.db.query(u"""SELECT a.*, 'country' as 'ltype', c.population
        #                     FROM allcountries as a
        #                     INNER JOIN alternatenames as b ON a.geonameid=b.geonameid
        #                     INNER JOIN allcities as c ON a.geonameid=c.id
        #                     WHERE
        #                     (country=? OR b.alternatename=?) LIMIT 1""", (name, name))

    def _querystate(self, name):
        """
        Check if name is an admin name
        """
        return [GeoPoint(**l) for l in AdminDB.query(name)]
        #stmt = u"""SELECT a.geonameid, a.name as 'admin1', b.country as 'country',
        #           'admin1' as 'ltype', 'ADM1' as 'featureCOde', 'A' as 'featureClass',
        #           b.ISO as 'countryCode', c.population
        #           FROM allcountries as b INNER JOIN alladmins as a on
        #           substr(a.key, 0, 3)=b.ISO
        #           INNER JOIN allcities as c ON c.id=a.geonameid
        #           WHERE (a.name=? or a.asciiname=?)
        #           """
        #return self.db.query(stmt, (name, name))

    def _querycity(self, name):
        """
        Check if name is a city name
        """
        stmt = u"""SELECT id as geonameid, name,
               population, latitude, longitude, countryCode,
               admin1, admin2, featureClass, featureCOde
               FROM allcities WHERE
               (name=? or asciiname=?) and featureClass != ""
               """
               #(a.featureCOde="ADM2" or a.featureCOde="ADM3" or a.featureClass="P") and
        res = self.db.query(stmt, (name, name))
        return res

    def _query_alternatenames(self, name):
        """
        check if name matches alternate name
        """
        stmt = u"""SELECT a.id as geonameid, a.name,
                a.population, a.latitude, a.longitude, a.countryCode,
                a.admin1, a.admin2, a.featureClass, a.featureCOde
                FROM alternatenames as d
                INNER JOIN allcities as a ON a.id=d.geonameId
                WHERE
                d.alternatename=? and a.featureClass != ""
                """
                #(a.featureCOde="ADM2" or a.featureCOde="ADM3" or a.featureClass="P")

        res = self.db.query(stmt, (name,))
        return res

    def normalize_statenames(self, country, admin):
        # get unofficial state name
        stmt = """ SELECT b.asciiname, c.country FROM allcities AS a INNER JOIN allcountries as c
               ON a.countryCode=c.ISO INNER JOIN alladmins as b
               ON b.geonameid=a.id
               WHERE
               (a.name=? or a.asciiname=?) and c.country=?
               and a.featureClass="A"
               """
        sub_res = self.db.query(stmt, (admin, admin, country))
        if sub_res:
            if len(sub_res) > 1:
                log.warning("More results returned than necessary-{}".format(admin))

            admin = sub_res[0].asciiname
        return admin

    def get_locInfo(self, country=None, admin=None, city=None):
        """
        return full loc tuple of name, admin1 name, country, population,
        longitude etc.
        """
        if city and (city.lower() == "ciudad de mexico" or city.lower() == u"ciudad de méxico"):
            city = "mexico city"

        if not isempty(admin):
            admin = self.normalize_statenames(country, admin)

        stmt = u"""SELECT a.id as geonameid, a.name,
               a.population,a.latitude, a.longitude, c.country as 'country',
               b.name as 'admin1', a.featureClass,
               a.featureCOde, a.countryCode as 'countryCode'
               FROM allcities a
               INNER JOIN alladmins b ON a.countryCode||"."||a.admin1 = b.key
               INNER JOIN allcountries c ON a.countryCode=c.ISO
               WHERE """

        if isempty(city) and isempty(admin):
            stmt += u"""c.country=? ORDER BY a.population DESC LIMIT 1"""
            params = (country,)

        elif isempty(city):
            stmt += u""" a.featureCOde == "ADM1" and (b.name=? or b.asciiname=?)
                    and c.country=? ORDER BY a.population DESC LIMIT 1"""
            params = (admin, admin, country)
        else:
            stmt += u""" (a.name=? or a.asciiname=?) and
                         (b.name=? or b.asciiname=?) and
                         c.country=?
                         ORDER BY a.population DESC LIMIT 1"""

            params = (city, city, admin, admin, country)

        res = self.db.query(stmt, params)
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
               LEFT OUTER JOIN alladmins b ON a.countryCode||"."||a.admin1 = b.key
               INNER JOIN allcountries c ON a.countryCode=c.ISO
               WHERE """

        if isempty(city) and isempty(admin):
            stmt += u"""c.country=? ORDER BY a.population DESC"""
            params = (country, )
        elif isempty(city):
            stmt += u"""alternatename=?
                       and a.featureCOde == "ADM1"
                       and c.country=? ORDER BY a.population DESC""".format(admin, country)
            params = (admin, country)
        else:
            stmt += u"""alternatename=?
                        and c.country=? ORDER BY a.population DESC"""
            params = (city, country)

        return self.db.query(stmt, params)

    def get_locById(self, locId):
        stmt = u"""SELECT a.id as geonameid, a.name,
               a.population,a.latitude, a.longitude, c.country as 'country',
               b.name as 'admin1',
               a.featureCOde, a.featureClass, a.countryCode as 'countryCode'
               FROM allcities a
               INNER JOIN alladmins b ON a.countryCode||'.'||a.admin1 = b.key
               INNER JOIN allcountries c ON a.countryCode=c.ISO
               WHERE
               a.id=?
               """

        return self.db.query(stmt, (locId,))

    def get_country(self, cc2):
        res = self.db.query("""SELECT *, 'country' as 'ltype' FROM
                            allcountries where ISO=?""", (cc2,))
        for l in res:
            l.confidence = 0.50

        return res


class MOD_GeoNames(BaseGazetteer):
    """
    Geonames in single table
    """
    def __init__(self, dbname, collectionName):
        self.db = MongoDBWrapper(dbname, collectionName)

    def query(self, name, min_popln=0):
        pass
