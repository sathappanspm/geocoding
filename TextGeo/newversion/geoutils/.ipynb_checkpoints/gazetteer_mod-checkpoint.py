#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from .dbManager import ESWrapper
from . import GeoPoint, blacklist, loc_default, CountryDB, AdminDB, FEATURE_WEIGHTS
from .loc_config import reduce_stopwords, remove_administrativeNames
from pylru import lrudecorator
import logging
import ipdb

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
    def __init__(self, db, priority=None, confMethod='Population', escore=True):
        self.db = db  # SQLiteWrapper(dbpath)
        self.escore = escore

        if priority is None:
            # TODO: Define default priority
            self.priority = None
        else:
            self.priority = priority

        if isinstance(db, ESWrapper):
            self._query = self.esquery

        self._get_loc_confidence = getattr(self, '_get_loc_confidence_by{}'.format(confMethod))

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
        #name = remove_administrativeNames(name.lower())
        if name in loc_default:
            name = loc_default[name]

        if name in blacklist:
            return []

        country = self._querycountry(name)
        if country == []:
            admin = self._querystate(name, **kwargs)
            city_alt = self.db.query(name, qtype=kwargs.pop("qtype", "cross_fields"),
                                     query_name="should",
                                     min_popln=min_popln, **kwargs)
        else:
            admin, city_alt = [], []

        ldist = (city_alt + country + admin)
        return ldist

    @lrudecorator(1000)
    def query(self, name, min_popln=0, **kwargs):
        reduceFlag = kwargs.pop('reduce', False)
        ldist = self._query(name, min_popln=min_popln, **kwargs)
        ldist = self._get_loc_confidence(ldist, min_popln)

        if ldist == [] and reduceFlag:
            name = reduce_stopwords(name)
            ldist = self.query(name, min_popln=min_popln, **kwargs)

        return ldist

    def _get_loc_confidence_byPopulation(self, ldist, min_popln):
        popsum = sum([(float(l.population) + 10.0) for l in ldist])
        rmset = set()
        for idx, l in enumerate(ldist):
            if l.featureClass.lower() == "l":
                rmset.add(idx)

            l.confidence = ((float(l.population) + 10.0) / popsum)
            if self.escore:
                l.confidence = l.confidence * self.escore

        if len(rmset) < len(ldist):
            ldist = [l for idx, l in enumerate(ldist) if idx not in rmset]
            popsum = sum([(float(l.population) + 10.0) for l in ldist])
            for l in ldist:
                l.confidence = ((float(l.population) + 10.0) / popsum)
                if self.escore:
                    l.confidence = l.confidence * self.escore

        return ldist

    def _get_loc_confidence_byUniform(self, ldist, min_popln):
        rmset = set()
        for idx, l in enumerate(ldist):
            if l.featureClass.lower() == "l":
                rmset.add(idx)

            l.confidence = (1.0 / len(ldist))
            if self.escore:
                l.confidence = l.confidence * self.escore

        if len(rmset) < len(ldist):
            ldist = [l for idx, l in enumerate(ldist) if idx not in rmset]
            for l in ldist:
                l.confidence = (1.0 / len(ldist))
                if self.escore:
                    l.confidence = l.confidence * self.escore

        return ldist

    def _get_loc_confidence_byHierarchy(self, ldist, min_popln):
        rmset = set()
        featuresum = sum([FEATURE_WEIGHTS.get(l.featureCode, 0.05) for l in ldist])
        for idx, l in enumerate(ldist):
            if l.featureClass.lower() == "l":
                rmset.add(idx)

            l.confidence = (FEATURE_WEIGHTS.get(l.featureCode, 0.05) / featuresum)
            if self.escore:
                l.confidence = l.confidence * self.escore

        if len(rmset) < len(ldist):
            ldist = [l for idx, l in enumerate(ldist) if idx not in rmset]
            featuresum = sum([FEATURE_WEIGHTS.get(l.featureCode, 0.05) for l in ldist])
            for l in ldist:
                l.confidence = (FEATURE_WEIGHTS.get(l.featureCode, 0.05) / featuresum)
                if self.escore:
                    l.confidence = l.confidence * self.escore

        return ldist

    def _querycountry(self, name):
        """
        Check if name is a country name
        """
        return [GeoPoint(**l) for l in CountryDB.query(name)]

    def _querystate(self, name, **filterargs):
        """
        Check if name is an admin name
        """
        result = [GeoPoint(**l) for l in AdminDB.query(name)]
        def filtercheck(gp):
            flag = True
            for flt in filterargs:
                # these args are not relevant for admin
                if flt not in gp.__dict__:
                    continue

                args = filterargs[flt]
                if flt == 'featureCode':
                    flag = flag & (flt in gp.__dict__)  & (gp.__dict__.get(flt)[:4].lower() in args)
                else:
                    flag = flag & (flt in gp.__dict__) & (gp.__dict__.get(flt).lower() in args)

            return flag

        return [r for r in result if filtercheck(r)]

    def get_locInfo(self, country=None, admin=None, city=None):
        """
        return full loc tuple of name, admin1 name, country, population,
        longitude etc.
        """
        corrections = {u"united states of america": u"united states",
                       u"korea (south)": u"south korea"}
        pts = []
        if country is None:
            raise Exception('use query if country info is not available')

        country = country.lower()
        country = corrections.get(country, country)
        countryInfo = self._querycountry(country) or self._query(country, featureCode='pcli')

        if countryInfo in ('', None, []):
            return []

        co = countryInfo[0]
        country = co.name
        countrycode = co.countryCode
        pts = None

        if city not in (None, '', '-'):
            city = city.lower()
            pts = (self.db.query(city, qtype='phrase', countryCode=countrycode.lower()) +
                   self.db.query(city, countryCode=countrycode.lower()))

            pts = [p for p in pts if p.featureCode[:4] != 'adm1']
            # pts = [p for p in pts if (p.featureCode[:3] in ("pcl", "ppl") or
            #                           p.featureCode in ('adm3', 'adm4'))]

        # ipdb.set_trace()
        if admin not in (None, '', '-'):
            admin = admin.lower()
            admins = self.query(admin,
                                countryCode=countrycode.lower(), featureCode='adm1')

            if city not in (None, []):
                adms = [al.lower() for l in admins for al in l.__dict__.get('alternatenames', [l.admin1])
                        if l.featureCode.lower()[:4] in ('adm1', 'ppla')]

                pts = [p for p in pts if p.admin1.lower() in adms]
            if pts in ([], None):
                pts = [l for l in admins
                       if l.featureCode.lower()[:4] in ('adm1', 'ppla')]

        return sorted([l.__dict__ for l in pts], key=lambda x: x['_score'], reverse=True)

    def get_locById(self, locId):
        return self.db.getByid(locId)

    def get_country(self, cc2):
        gpt = CountryDB.fromISO(cc2)
        if gpt is not None:
            gpt = [GeoPoint(**gpt)]
        else:
            gpt = []

        return gpt