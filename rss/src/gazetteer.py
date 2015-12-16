#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from dbManager import SQLiteWrapper
import pandas as pd
import ipdb


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


class Geonames(BaseGazetteer):
    def __init__(self, dbpath, dbname, tables=None, priority=None):
        self.db = SQLiteWrapper(dbpath)
        self.dbname = dbname
        if tables is None:
            self.tables = ('allCities', 'allAdmins', 'allCountries')
        else:
            self.tables = tables

        if priority is None:
            # TODO: Define default priority
            self.priority = None
        else:
            self.priority = priority

    def query(self, name, absoluteMatch=False):
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

        country = self._querycountry(name)
        admin = self._querystate(name)
        city = self._querycity(name)
        #res = self.score(city, admin, country)
        return (city, country, admin)

    def _querycountry(self, name):
        """
        Check if name is a country name
        """
        return self.db.query(u"SELECT * FROM allcountries WHERE country='{0}'".format(name))

    def _querystate(self, name):
        """
        Check if name is an admin name
        """
        stmt = u"SELECT * FROM alladmins WHERE name='{0}' or asciiname='{0}'".format(name)
        return self.db.query(stmt)

    def _querycity(self, name):
        """
        Check if name is a city name
        """
        stmt = u"SELECT * FROM allcities WHERE name='{0}' or asciiname='{0}'".format(name)
        res = self.db.query(stmt)
        if res:
            df = pd.DataFrame(res)
            df['population'] = (df['population'] + 1).astype(float)
            df['confidence'] = (df['population']) / (2 * (df['population'].sum())) + 0.5
            res = df.to_dict(orient='records')

        return res

    def score(self, city, admin, country):
        """
        score each entry based on priority policy
        """
        # TODO: implement priority policy

        scoresheet = {}

        def update(locInfo):
            fulltuple = '//'.join([locInfo['name'], locInfo['countryCode'], locInfo['admin1']])
            st_tuple = '//' + locInfo['countryCode'] + '//' + locInfo['admin1']
            cotuple = '//' + locInfo['countryCode'] + '//'
            pvalue = locInfo['confidence'] ** 2
            if fulltuple not in scoresheet:
                scoresheet[fulltuple] = 0.0

            if st_tuple not in scoresheet:
                scoresheet[st_tuple] = 0.0

            if cotuple not in scoresheet:
                scoresheet[cotuple] = 0.0

            scoresheet[fulltuple] += pvalue
            scoresheet[st_tuple] += pvalue * 0.7
            scoresheet[cotuple] += pvalue * 0.5
            return

        _ = [update(ci) for ci in city]
        for a in admin:
            key = "//" + a['key'].replace(".", "//")
            if key not in scoresheet:
                scoresheet[key] = 0.5
            else:
                scoresheet[key] += scoresheet[key]

        for co in country:
            key = "//" + co['ISO'] + "//"
            if key not in scoresheet:
                scoresheet[key] = 0.5
            else:
                scoresheet[key] += scoresheet[key]
        return scoresheet

    def get_locInfo(self, locId):
        """
        return full loc tuple of name, admin1 name, country, population,
        longitude etc.
        """
        return self.db.query("SELECT * FROM geonames_fullText where id='{}'".format(locId))
