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
from .loc_config import reduce_stopwords, remove_administrativeNames, stop_words as STOP_WORDS
# from pylru import lrudecorator
import logging
import ipdb
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import Search, Q
import re

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

        #if isinstance(db, ESWrapper):
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
                                     min_popln=min_popln, **kwargs)
        else:
            admin, city_alt = [], []

        ldist = (city_alt + country + admin)
        return ldist

    # @lrudecorator(1000)
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
            # if l.featureClass.lower() == "l":
            #    rmset.add(idx)

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
            #if l.featureClass.lower() == "l":
            #    rmset.add(idx)

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
            #if l.featureClass.lower() == "l":
            #    rmset.add(idx)

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
        countryInfo = (self._querycountry(country))

        if countryInfo in ('', None, []):
            return []

        co = countryInfo[0]
        country = co.name
        countrycode = (co.countryCode)
        pts = None
        ctys = []
        if city not in (None, '', '-'):
            city = city.lower()
            pts = (self.db.query(city, countryCode=countrycode.lower()))
            ctys = pts
            pts = [p for p in pts if p.featureCode[:4].lower() != 'adm1'
                   and (p.name.lower() == city or p.asciiname.lower() == city or city in p.alternatenames)]
            if not pts:
                pts = self.db.query(city, countryCode=countrycode.lower(), fuzzy='AUTO')
                pts = [p for p in pts if p.featureCode[:4].lower() != 'adm1']

        if admin not in (None, '', '-'):
            admin = admin.lower()
            admins = self.db.query(admin,
                                   countryCode=countrycode.lower(),
                                   featureCode=['adm1', 'adm1h'])

            if city not in (None, []):
                adms = [al.lower() for l in admins for
                        al in l.__dict__.get('alternatenames', [l.admin1])
                        if l.featureCode.lower()[:4].lower() in ('adm1', 'ppla')]

                pts = [p for p in pts if p.admin1.lower() in adms]
                if pts == []:
                    tmp = (self.db.query(city, countryCode=countrycode.lower(),
                            admin1=admins[0].admin1Code, fuzzy='AUTO'))
                    if tmp:
                        pts = ctys
                    elif (adms[0] in ctys[0].alternatenames or admin in ctys[0].alternatenames):
                        pts = admins

                    elif (ctys[0].name == city.lower() or
                          ctys[0].asciiname == city.lower() or
                          city.lower() in ctys[0].alternatenames):
                        pts = ctys
            else:
                pts = admins
        else:
            npts = [p for p in pts if p.admin1 in (None, '', '-')]
            if npts:
                pts = npts

        if pts in (None, []):
            raise Exception('no geocode')

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


class Gazetteer(GeoNames):
    def __init__(self, db, **kwargs):
        self.conn = Search(using=Elasticsearch(urls='http://localhost', port=9200),
                           index='geonames', doc_type='places')
        self.tokenizer = re.compile(' |\||,|_|-')
        super(Gazetteer, self).__init__(db, **kwargs)

    def query_es(self, place, **filterargs):
        res = self.dbquery(place, **filterargs)
        if res.hits.total == 0:
            placetokens = [l.strip() for l in self.tokenizer.split(place) if l and l not in STOP_WORDS]
            reduced_placename = " ".join(placetokens)
            if len(placetokens[0]) <= 3 and len(placetokens) > 1 and 3.0 / len(placetokens) >= .5:
                reduced_placename = " ".join(placetokens[1:])

            res = self.dbquery(reduced_placename, **filterargs)
            if res.hits.total == 0:
                res = self.dbquery(reduced_placename, fuzzy=2)

        return [GeoPoint(**r._d_) for r in res.hits]

    def query(self, name, **kwargs):
        name = name.strip().lower()
        #name = remove_administrativeNames(name.lower())
        if name in loc_default:
            name = loc_default[name]

        if name in blacklist:
            return []

        country = self._querycountry(name)
        if country == []:
            admin = self._querystate(name, **kwargs)
            city_alt = self.query_es(name, **kwargs)
        else:
            admin, city_alt = [], []

        ldist = (city_alt + country + admin)
        ldist = self._get_loc_confidence(ldist, 0)
        return ldist

    def dbquery(self, place, qtype='best_fields', **kwargs):
        q = {"multi_match": {"query": place,
             "fields": ['name^5', 'asciiname^5', 'alternativenames'],
             "type": qtype}
            }
        if 'fuzzy' in kwargs:
            q['multi_match'].pop("type")
            fuzzy = {"fuzziness": kwargs.pop('fuzzy', 0),
                     "max_expansions": kwargs.pop('max_expansion', 10),
                     "prefix_length": kwargs.pop('prefix_length', 1),
                     "operator": "and"}
            q['multi_match'].update(fuzzy)

        res = self.conn.query(q)
        if kwargs:
            if 'min_popln' in kwargs:
                res = res.filter("range", population={"gte": kwargs.pop("min_popln")})
            for item in kwargs:
                if not isinstance(kwargs[item], str):
                    qname = "terms"
                else:
                    qname = "term"

                res = res.filter(qname, **kwargs)

        res = res[0:20].execute()
        return res
