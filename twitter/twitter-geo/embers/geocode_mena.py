#!/usr/bin/env pyth_n
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog
"""

__author__ = ['Patrick Butler', 'Rupinder Paul']
__email__ = ['pabutler@vt.edu', 'rupen@cs.vt.edu']
__version__ = "0.2.4"


import re
import os
import csv
import copy
import hashlib
import numpy as np
from scipy.spatial import KDTree
from math import sin, cos, radians, atan2
from collections import deque, namedtuple

from etool import logs
from embers.utils import normalize_str as nstr
from geo_utils import *
from StringMatcher import StringMatcher

WG_DATA = os.path.join(os.path.dirname(__file__), "world-gazetteer-mena.v1.txt")
# NOTE:lac_co_admin - contains info on all latin-american countries
CO_ADMIN_DATA = os.path.join(os.path.dirname(__file__), "mena_co_admin.txt")
# landmarks having high likelihood to specific locations in GSR data
LANDMARKS_DATA = os.path.join(os.path.dirname(__file__), "mena_landmarks_0.txt")
# dirty landmarks having high likelihood to specific locations in GSR data
DIRTY_LANDMARKS_DATA = os.path.join(os.path.dirname(__file__), "mena_landmarks_0.txt")

# NOTE
# padded siginifies extra coordinates added for parent record
# recieved from Graham@CACI
# padded=0 - orginal world gazeteer record
# padded=1 extra padded cooridinates for locations from Yahoo API
MENA_WG_FIELDS = ["id", "name", "alt_names", "orig_names", "type", "pop",
             "latitude", "longitude",
             "country", "admin1", "admin2", "admin3",
             "padded", "source", "matched"]

DIRTY_LANDMARKS_FIELDS = ["name", "alias", "city", "country", "admin1",
                          "admin2", "admin3", "latitude", "longitude", "id",
                          "location_string", "location_score", "total_score"]

LANDMARKS_FIELDS = ["id", "name", "city", "admin1", "country",
                    "latitude", "longitude", "location_score"]

CO_ADMIN_FIELDS = ["id", "type", "name", "full_name", "alt_names",
                   "capital_city", "latitude", "longitude", "iso_3166_code"]

GeocodingResolution = namedtuple("GeocodingResolution", "city admin1 country")
RESOLUTION_LEVEL = GeocodingResolution(city=0, admin1=1, country=2)

PRIORITY_POLICY = ['geo_coordinates', 'geo_place', 'user_location_rnt', 'user_location_rni',
                   'user_location']

LAT_LON_RE = re.compile("([-+]?\d{1,2}.\d+)\s*[,\s*]\s*([-+]?\d{1,3}.\d+)")

FUZZY_MATCH = lambda x, y: StringMatcher(seq1=x, seq2=y).ratio()


class ManyRE(object):
    """a class for generating a large RE for searching for text"""
    def __init__(self, res):
        """@todo: to be defined
        :param res: a list of regular expressions to combine
        """
        self.lookup = {}

        self._res = res
        try:
            for r in self._res:
                words = tuple(i for i in re.split("\W+", r) if len(i) > 1)
                if words:            
                    w = words[0]
                    if w not in self.lookup:
                        self.lookup[w] = []
                    self.lookup[w] += [words]
        except:
            raise

    def search(self, s):
        s = s.replace("_", " ")
        words = [i for i in re.split("\W+", s) if len(i) > 1]
        for i, w in enumerate(words):
            if w not in self.lookup:
                continue

            for lookup in self.lookup[w]:
                for j, w in enumerate(lookup[1:]):
                    k = i + j + 1
                    if k >= len(words) or w != words[k]:
                        break
                else:
                    return " ".join(lookup)
        return None


def float_or_none(s):
    if s:
        return float(s)
    return None


def int_or_none(s):
    if s:
        return int(s)
    return None


def object_or_none(s):
    if s:
        return s
    return None


def get_wg_data(wg_data):
    if wg_data == WG_DATA:
        if "__loader__" in globals():
            import cStringIO
            f = __loader__.get_data("embers/" + os.path.basename(WG_DATA))
            f = cStringIO.StringIO(f)
        else:
            f = open(WG_DATA)
    else:
        f = open(wg_data)
    return f


def get_co_admin_data(co_admin_data):
    if co_admin_data == CO_ADMIN_DATA:
        if "__loader__" in globals():
            import cStringIO
            f = __loader__.get_data("embers/" + os.path.basename(CO_ADMIN_DATA))
            f = cStringIO.StringIO(f)
        else:
            f = open(CO_ADMIN_DATA)
    else:
        f = open(co_admin_data)
    return f


def get_landmarks_data(landmarks_data):
    if landmarks_data == LANDMARKS_DATA:
        if  "__loader__" in globals():
            import cStringIO
            f = __loader__.get_data("embers/" + os.path.basename(LANDMARKS_DATA))
            f = cStringIO.StringIO(f)
        else:
            f = open(LANDMARKS_DATA)
    else:
        f = open(landmarks_data)
    return f


def get_dirty_landmarks_data(dirty_landmarks_data):
    if dirty_landmarks_data == DIRTY_LANDMARKS_DATA:
        if "__loader__" in globals():
            import cStringIO
            f = __loader__.get_data("embers/" + os.path.basename(DIRTY_LANDMARKS_DATA))
            f = cStringIO.StringIO(f)
        else:
            f = open(DIRTY_LANDMARKS_DATA)
    else:
        f = open(dirty_landmarks_data)
    return f


class GeoMena(object):
    """A Geocoding class for tweets
    WARNING: This class should be created a minimum number of
    times!  The initialization of this class takes several seconds
    Features:
        please refer to README-geocode.md in twitter/embers/
    """
    def __init__(self, wg_data=WG_DATA, co_admin_data=CO_ADMIN_DATA,
                 landmarks_data=LANDMARKS_DATA, dirty_landmarks_data=DIRTY_LANDMARKS_DATA,
                 priority_policy=PRIORITY_POLICY, use_dirty_landmarks=False, 
                 debug=False):
        """
        """
        self.use_dirty_landmarks = use_dirty_landmarks
        self.priority_policy = priority_policy
        self.debug = debug

        self.__version__ = "{0}-{1}-{2}-{3}-{4}".format(
            self.__class__.__name__,
            __version__,
            hashlib.md5(get_wg_data(wg_data).read()).hexdigest(),
            hashlib.md5(get_co_admin_data(co_admin_data).read()).hexdigest(),
            hashlib.md5(" ".join(self.priority_policy)).hexdigest())

        if self.debug:
            try:
                logs.init()
            except IOError:
                logs.init(logfile=self.__class__.__name__.lower())

            self.log = logs.getLogger("{0}-{1}".format(
                                      self.__class__.__name__,
                                      __version__.replace('.', '_')))

        self.nstr = lambda x:x
        # 1a. load landmarks
        self.landmarks = {}
        try:
            f = get_landmarks_data(landmarks_data)
            dialect = csv.Sniffer().sniff(f.read(10240), delimiters="\t")
            f.seek(0)
            reader = csv.DictReader(f, dialect=dialect,
                                    fieldnames=LANDMARKS_FIELDS)
            for r in reader:
                for k in r.keys():
                    r[k] = r[k].strip() if r[k] else r[k]
                # TODO include aliases in file since we are 
                # not lower casing keys 
                self.landmarks[self.nstr(r['name'])] =\
                   [object_or_none(r['city']), r['country'],
                    object_or_none(r['admin1']), None ,
                    None, 0, float_or_none(r['latitude']),
                    float_or_none(r['longitude']), int_or_none(r["id"]), 0,
                    r['name'], float_or_none(r["location_score"])]

            f.close()
        except Exception, er:
            pass

        # 1b. load "dirty" landmarks
        self.dirty_landmarks = {}
        if use_dirty_landmarks:
            try: 
                f = get_dirty_landmarks_data(dirty_landmarks_data)
                dialect = csv.Sniffer().sniff(f.read(10240), delimiters="\t")
                f.seek(0)
                reader = csv.DictReader(f, dialect=dialect,
                                        fieldnames=DIRTY_LANDMARKS_FIELDS)
                for r in reader:
		    for k in r.keys():
			r[k] = r[k].strip() if r[k] else r[k]
		    # TODO include aliases in file since we are 
		    # not lower casing keys 
		    self.dirty_landmarks[self.nstr(r['name'])] =\
		       [object_or_none(r['city']), r['country'],
			object_or_none(r['admin1']), None ,
			None, 0, float_or_none(r['latitude']),
			float_or_none(r['longitude']), int_or_none(r["id"]), 0,
			r['name'], float_or_none(r["location_score"])]

		f.close()
            except Exception, er:
                pass

        # 2. load country and admin1 level geo data
        # prep lookup dictionaries
        # key__value

        # countries
        self.co_code = {}
        self.co_names = {}
        self.co_aliases = {}
        self.co_capital_cities = {}
        # admin1
        self.admin_code = {}
        self.admin_name = {}
        # assumes countries appear first when reading data from
        # lac_co_admin TODO BAD!
        try:
            f = get_co_admin_data(co_admin_data)
            dialect = csv.Sniffer().sniff(f.read(10240), delimiters="\t")
            f.seek(0)
            reader = csv.DictReader(f, dialect=dialect, fieldnames=CO_ADMIN_FIELDS)

            for r in reader:
                for k in r.keys():
                    r[k] = r[k].strip()
                lat = float_or_none(r['latitude'])
                lon = float_or_none(r['longitude'])
                code = object_or_none(r['iso_3166_code'])
                rid = int_or_none(r["id"])
                if r['type'] == 'country':
                    # country
                    if code:
                        self.co_code[code] = r['name']
                        self.co_names[self.nstr(r['name'])] = (rid, lat, lon,
                                                          code, r['name'])
                        self.co_capital_cities[self.nstr(r['capital_city'])] =\
                            (r['capital_city'], r['name'])
                        aliases = r['alt_names'].split(',')
                        self.co_aliases.update({self.nstr(alias.strip()): r['name']
                                                for alias in aliases})
                    else:
                        if self.debug:
                            self.log.error("Bad data country {0} Code {1}".format(
                                           r['name'], code))
                elif r['type'] == 'admin':
                    # admin
                    admin, co = r['full_name'].split(',')
                    admin, co = admin.strip(), co.strip()

                    if code:
                        if code not in self.admin_code:
                            self.admin_code[code] = []
                        self.admin_code[code].append((co, admin))
                    co1, a = self.nstr(co), self.nstr(admin)
                    if a not in self.admin_name:
                        self.admin_name[a] = {}
                    if co1 not in self.admin_name[a]:
                        self.admin_name[a][co1] = (rid, lat, lon, code, admin, co)

            f.close()
        except Exception, er:
            pass

        # 3. load (world-gazeteer) city level geo data
        f = get_wg_data(wg_data)
        dialect = csv.Sniffer().sniff(f.read(10240), delimiters="\t")
        f.seek(0)
        reader = csv.DictReader(f, dialect=dialect, quoting=csv.QUOTE_NONE, fieldnames=MENA_WG_FIELDS)
        self.ci_aliases = {}
        # main data store for geocoding
        self.data = []
        counter = 0
        ci_set = set()
        for r in reader:
            for k in r.keys():
                r[k] = r[k].strip()
            # get alias names for cities
            ci_names = [a.strip() for a in r['alt_names'].split(',')
                        if len(a.strip()) > 0]
            ci_names.extend([a.strip() for a in r['orig_names'].split(',')
                             if len(a.strip()) > 0])
            for ci in ci_names:
                k = (self.nstr(ci), self.nstr(r['country']))
                if len(k[0]) == 0:
                    print ci, k
                a1 = self.nstr(r['admin1'])
                if k not in self.ci_aliases:
                    self.ci_aliases[k] = {a1: set([r['name']])}
                elif a1 not in self.ci_aliases[k]:
                    self.ci_aliases[k][a1] = set([r['name']])
                else:
                    # Cases where different cities for same
                    # admin-country pair have the same alias
                    self.ci_aliases[k][a1].add(r['name'])
                # add ci name aliases into ci_set
                ci_set.add(self.nstr(ci))
            # store only cannonical cities names
            self.data.append((counter, (r['name'], r['country'],
                              r['admin1'],
                              object_or_none(r['admin2']),
                              object_or_none(r['admin3']),
                              int_or_none(r['pop']),
                              float_or_none(r['latitude']) / 100,
                              float_or_none(r['longitude']) / 100,
                              int(r['id']), int(r['padded']))))
            counter += 1

        self.coordinates = {}
        # cases where admin1 and city share the same name
        # extended feature/hack #1 to resolve city when
        # only country and admin1 are specified
        self.same_ci_a1_name = {}
        for i, (n, c, a1, a2, a3, p, lat, lon, i_d, pad) in self.data:
            nn, nc, na1 = self.nstr(n),  self.nstr(c), self.nstr(a1)
            self.coordinates[(lat, lon)] = i
            if nn == na1 and pad == 0:
                self.same_ci_a1_name[(nc, na1)] = n
            ci_set.add(nn)

        # store (lat, lon)
        self.kdtree = KDTree([[i, j] for i, j in self.coordinates.keys()
                              if i is not None and j is not None])
        # build regular expr dicts
        co_set = set(self.co_names.keys())
        # add country name aliases into co_set
        co_set.update(self.co_aliases.keys())
        self.co_reg = ManyRE(co_set)
        self.ci_reg = ManyRE(ci_set)
        # add admin1 name aliases into admin1_set
        admin1_set = set(self.admin_name.keys())
        # build regular expression stores for co-admin1-ci
        self.admin1_reg = ManyRE(admin1_set)
        # add stopwords to prevent any 2-letter word in common usage
        # to be mis-interpretted as country or admin code
        two_letter_stop_words = set(
            ['BE', 'WE', '\xc3\xa0', 'YO', 'DO', 'YA', 'DE', 'DA', 'HA', 'BY',
             'HE', 'AL', 'NI', 'LE', 'NO', 'LO', 'TU', 'TO', 'TI', 'TE', 'EM',
             'EL', 'EN', 'IS', 'OS', 'AM', 'IT', 'AO', 'AN', 'AS', 'AT', 'IN',
             'EU', 'ES', 'IF', 'ME', 'ON', 'OF', 'LA', 'MI', 'UP', 'SU', 'UM',
             'UN', 'SO', 'NA', 'OU', 'MY', 'OR', 'SE', 'US'])

        self.co_code_reg = ManyRE([sw for sw in self.co_code.keys()
                                  if sw not in two_letter_stop_words])
        self.admin1_code_reg1 = ManyRE(self.admin_code.keys())
        self.admin1_code_reg2 = ManyRE([sw for sw in self.admin_code.keys()
                                       if sw not in two_letter_stop_words])

        self.bguess = {}
        for i, (city, country, admin1, a2, a3, p, la, lo, i_d, pad)\
                in self.data:

            ci, co, a = self.nstr(city), self.nstr(country), self.nstr(admin1)
            # value is list of admin1's that correspond to ci-co key
            # ci-co makes dictionary flatter
            # choose not to use co-admin1-ci as key to add more flexibility
            # for lookups
            if ci in self.bguess:
                if co in self.bguess[ci]:
                    if a in self.bguess[ci][co]:
                        # store original wg-records marked with pad = 0
                        # to head of the queue
                        if pad == 0:
                            self.bguess[ci][co][a].appendleft(i)
                        else:
                            self.bguess[ci][co][a].append(i)
                    else:
                        self.bguess[ci][co][a] = deque([i])
                else:
                    self.bguess[ci][co] = {a: deque([i])}
            else:
                self.bguess[ci] = {co: {a: deque([i])}}

    def _geo_from_landmarks(self, tweet_text):
        landmarks = []
        text = self.nstr(tweet_text)
        for l, v in self.landmarks.items():
            if l in text:
                landmarks.append(v)
                tweet_text.replace(l, " ",1)

        return landmarks

    def _geo_from_dirty_landmarks(self, tweet_text):
        landmarks = []
        text = self.nstr(tweet_text)
        for l, v in self.dirty_landmarks.items():
            if l in text:
                landmarks.append(v)
                tweet_text.replace(l, " ",1)

        return landmarks

    def geo_from_landmarks(self, tweet_text, dirty=False):
        """Do look up based on landmarks
        :param tweet_text: tweet's text
        :returns: (city, country, admin1,
                   admin2, admin3, pop, lat, lon, id, pad)
        """
        if dirty:
            landmarks = self._geo_from_dirty_landmarks(tweet_text)
        else:
            landmarks = self._geo_from_landmarks(tweet_text)
        if len(landmarks) > 0:
            # get landmark with most likelihood
            # to be related to a location
            return tuple(max(landmarks, key=lambda x:x[-1])[:-2])
        else:
            return None

    def get_all_cities(self, country, admin1):
        '''Return all cities for country & admin1 pair
        :param country: country name
        :param admin1: admin1 name
        :returns: list of city names
        '''
        cities = set()
        co = self.nstr(country)
        a = self.nstr(admin1)
        for ci, co_a in self.bguess.items():
            if co and co in co_a and a and a in co_a[co]:
                cities.add(self.data[co_a[co][a][0]][1][0])

        return list(cities)

    def best_guess_country(self, country):
        '''Resolve country
        :param country: norm str of country name
        :returns: ([LT], [])
                  country level geocoded tuple with lat-lon
                  LT = (city, country, admin1,
                        admin2, admin3, pop, lat, lon, id, pad)
        NOTE: lat-lon corresponds to avg. of lat, lon points from each
              record in world-gazetteer for given country
        '''
        co = self.nstr(country) if country else None
        if co and co in self.co_names:
            rid, lat, lon, code, co_name = self.co_names[co]
            return ([(None, co_name, None, None, None,
                      None, lat, lon, rid, 0)], [])
        else:
            return ([], [])

    def best_guess_admin1(self, admin1, country):
        '''Resolve country and admin1
        :param admin1: norm str of admin1 name
        :param country: norm str of country name
        :returns: ([LT], [])
                  admin1 level geocoded tuple with lat-lon
                  LT = (city, country, admin1,
                        admin2, admin3, pop, lat, lon, id, pad)
        NOTE: lat-lon corresponds to avg. of lat, lon points from each
              record in world-gazetteer for given country and admin pair
        '''
        a1, co = self.nstr(admin1), self.nstr(country)
        if a1 in self.admin_name and co in self.admin_name[a1]:
            rid, lat, lon, code, admin_name, co_name = self.admin_name[a1][co]
            return ([(None, co_name, admin_name, None, None,
                     None, lat, lon, rid, 0)], [])
        else:  # if not found return country level geocoding
            return self.best_guess_country(co)

    def _best_guess_city(self, city, country=None, admin1=None):
        '''Resolve city
        :param admin1: norm str of admin1 name
        :param country (optional): norm str of country name
        :param admin1 (optional): norm str of admin1 name
        :param res_level (optional): reolution level
        :returns: ([canonical LT], canononical_admin_match)
                  city/admin/country level geocoded tuple with lat-lon
                  LT = (city, country, admin1,
                        admin2, admin3, pop, lat, lon, id, pad)
                  canonical_admin_match : T/F
        '''
        ci = self.nstr(city.strip())
        co = self.nstr(country.strip()) if country else None
        a1 = self.nstr(admin1.strip()) if admin1 else None
        # best-guess'es
        # aim is to first search through canonical names of city-country keys
        # and then also search alias ci names store
        # finaly we merge both results, where priority is given to records
        # results from canonical name searches
        canonical_name_bgs = set()
        canonical_a_match = False

        if ci in self.bguess:
            if co and co in self.bguess[ci]:
                if a1:
                    # TODO add log in else
                    if a1 in self.bguess[ci][co]:
                        canonical_a_match = True
                        canonical_name_bgs.add(
                            self.data[self.bguess[ci][co][a1][0]][1])
                else:
                    canonical_name_bgs.update([self.data[indices[0]][1]
                                               for adm1, indices in
                                               self.bguess[ci][co].items()])
            elif not co:
                # pick if only one possible ci-co pair is present
                if len(self.bguess[ci]) == 1:
                    co1 = self.bguess[ci].keys()[0]
                    if a1:  # if admin is provided
                        # make sure its present in picked country
                        if a1 in self.bguess[ci][co1]:
                            canonical_a_match = True
                            canonical_name_bgs.add(
                                self.data[self.bguess[ci][co1][a1][0]][1])
                    else:
                        if len(self.bguess[ci][co1]) == 1:
                            canonical_name_bgs.update(
                                [self.data[indices[0]][1] for adm1, indices in
                                    self.bguess[ci][co1].items()])
                        else:  # do country level geocoding
                            bg_co = self.best_guess_country(co1)[0]
                            if bg_co:
                                canonical_name_bgs.add(bg_co[0])
                            else:
                                self.log.info(
                                    "Country not found CO:{0}".format(co1))
                else:  # if more than one country pair is present
                    if a1:
                        possible_co = set([co for co in
                                          self.bguess[ci].keys()
                                          if a1 in self.bguess[ci][co]])
                        if len(possible_co) == 1:
                            co1 = list(possible_co)[0]
                            canonical_name_bgs.add(
                                self.data[self.bguess[ci][co1][a1][0]][1])

        return canonical_name_bgs, canonical_a_match

    def best_guess_city(self, city, country=None, admin1=None,
                        res_level=RESOLUTION_LEVEL.city):
        '''Resolve city
        :param admin1: norm str of admin1 name
        :param country (optional): norm str of country name
        :param admin1 (optional): norm str of admin1 name
        :param res_level (optional): reolution level
        :returns: ([canonical LT], [alias LT])
                  city/admin/country level geocoded tuple with lat-lon
                  LT = (city, country, admin1,
                        admin2, admin3, pop, lat, lon, id, pad)
        '''
        ci = self.nstr(city.strip())
        co = self.nstr(country.strip()) if country else None
        a1 = self.nstr(admin1.strip()) if admin1 else None
        canonical_name_bgs, canonical_a_match = self._best_guess_city(ci,
                                                                      co, a1)
        all_bgs = copy.deepcopy(canonical_name_bgs)
        alias_name_bgs, loc = set(), set()
        alias_a_match = False
        k = (ci, co)
        if k in self.ci_aliases:
            if a1 and a1 in self.ci_aliases[k]:
                alias_a_match = True
                for o_ci in self.ci_aliases[k][a1]:
                    loc.add(((self.nstr(o_ci), co), a1))
            else:
                if self.debug and a1:
                    self.log.info("Alias key (%s) exists but not admin1 %s" %
                                  (list(k), a1))
                for adm1, o_cities in self.ci_aliases[k].items():
                    for o_ci in o_cities:
                        loc.add(((self.nstr(o_ci), co), adm1))
            #print loc
            for (ci, co), a in loc:
                alias_bgs, alias_a_found = self._best_guess_city(ci, co, a)
                for bg in alias_bgs:
                    if bg not in all_bgs:
                        all_bgs.add(bg)
                        alias_name_bgs.add(bg)

        # sort by specific priority
        # TODO sort by tweet volume
        alias_name_bgs = sorted(alias_name_bgs,
                                key=lambda x: x[5], reverse=True)
        canonical_name_bgs = sorted(canonical_name_bgs,
                                    key=lambda x: x[5], reverse=True)
        # if no results returned try resolving on either admin1 level first
        # if that fails do country level
        if len(canonical_name_bgs) == 0 and len(alias_name_bgs) == 0 and \
           res_level > RESOLUTION_LEVEL.city:
            if co and a1:
                return self.best_guess_admin1(a1, co)
            elif co:
                return self.best_guess_country(co)
        if not canonical_a_match and alias_a_match:
            return (list(alias_name_bgs), [])
        else:
            return (list(canonical_name_bgs), list(alias_name_bgs))

    def best_guess(self, city=None, admin1=None, country=None,
                   res_level=RESOLUTION_LEVEL.city):
        """Makes a best guess with  for a given
           city, country, admin1 tuple
        :param city: city name
        :param country: country name
        :param admin1: admin1, state or province name
        :returns: ([canonical LT], [alias LT])
        city/admin/country level geocoded tuple with lat-lon
        LT = (city, country, admin1,
              admin2, admin3, pop, lat, lon, id, pad)
        NOTE: lat-lon corresponds to that of city/capital
              of admin1 or country
        """
        ci, a1, co = (city and self.nstr(city), admin1 and self.nstr(admin1),
                      country and self.nstr(country))
        if co and co in self.co_aliases:
            country = self.co_aliases[co]
            co = self.nstr(self.co_aliases[co])
        # ref: hack#1 in features list
        if a1 and co and not ci:
            if (co, a1) in self.same_ci_a1_name:
                ci = self.nstr(self.same_ci_a1_name[(co, a1)])

        if co and not a1 and not ci and co in self.co_names:
            return self.best_guess_country(co)
        elif a1 and co and not ci:
            return self.best_guess_admin1(a1, co)
        elif ci:
            return self.best_guess_city(ci, co, a1, res_level)
        else:
            return ([], [])

    def lookup_capital_city(self, capital_city, country):
        if capital_city in self.co_capital_cities:
            ci, co = self.capital_cities[capital_city]
            return best_guess_city(capital_city, co)
        else:
            return None

    def lookup_city(self, lat1, lon1, maxd=30.):
        """Looks up the nearest city at most maxd distance from coordinates

        :param lon1: longitude to lookup
        :param lat1: latitude to lookup
        :param maxd: max distance from coordinate
        :returns: ((city, country, admin1,
                    admin2, admin3, pop, lat, lon), coordinates, distance)

        """
        i, d = self.kdtree.query([lat1, lon1], distance_upper_bound=maxd)
        if i == np.inf:
            return None
        nn = self.kdtree.data[d]
        lat2 = radians(nn[0])
        lon2 = radians(nn[1])
        lat1 = radians(lat1)
        lon1 = radians(lon1)
        dLat = lat2 - lat1
        dLon = lon2 - lon1

        R = 6371
        a = (sin(dLat / 2) * sin(dLat / 2) +
             sin(dLon / 2) * sin(dLon / 2) * cos(lat1) * cos(lat2))
        c = 2 * atan2(a ** .5, (1 - a) ** .5)
        d = R * c
        if d < maxd:
            i = self.coordinates[tuple(nn)]
            return self.data[i][1], nn, d
        else:
            return None

    def geo_from_lat_lon(self, lat, lon, radius=30.):
        """Do look up based on lat-lon coordinates
        :param lat: latitude
        :param lon: longitude
        :returns: (city, country, admin1,
                   admin2, admin3, pop, lat, lon, id, pad)
        """
        if lat is not None and lon is not None:
            city_t = self.lookup_city(lat, lon, radius)
            if city_t:
                return city_t[0]
        return None

    @staticmethod
    def geo_normalize_public(tweet):
        """ Normalizes tweet from the public streaming API
            in to geocodable chunks of information

        :param tweet: tweet payload
        :returns: lat, lon, places, texts
        """
        lon, lat, places, texts = None, None, {}, {}
        if 'place' in tweet and tweet['place'] and\
           'bounding_box' in tweet['place'] and tweet['place']['bounding_box']\
           and 'coordinates' in tweet['place']['bounding_box'] and\
           tweet['place']['bounding_box']['coordinates']:
            coords = tweet['place']['bounding_box']['coordinates'][0]
            n = float(len(coords))
            lon = sum(o for o, a in coords) / n
            lat = sum(a for o, a in coords) / n
            places = tweet['place']
        if 'coordinates' in tweet and tweet['coordinates'] and\
           'coordinates' in tweet['coordinates'] and\
           tweet['coordinates']['coordinates']:
            lon, lat = tweet['coordinates']['coordinates']

        if 'user' in tweet:
            if 'location' in tweet['user']:
                texts['user_location'] = tweet['user']['location']
            if 'description' in tweet['user']:
                texts['user_description'] = tweet['user']['description']
        if 'text' in tweet and tweet['text']:
            texts['text'] = tweet['text'].strip()

        return lat, lon, places, texts

    @staticmethod
    def geo_normalize_gnip(tweet):
        """ Normalizes tweet from the GNIP API
            in to geocodable chunks of information

        :param tweet: tweet payload
        :returns: lat, lon, places, texts
        """
        lon, lat, places, texts = None, None, {}, {}
        if 'location' in tweet and tweet['location'] and\
           'geo' in tweet['location'] and tweet['location']['geo']\
           and 'coordinates' in tweet['location']['geo'] and\
           tweet['location']['geo']['coordinates']:
            coords = tweet['location']['geo']['coordinates'][0]
            n = float(len(coords))
            lon = sum(o for o, a in coords) / n
            lat = sum(a for o, a in coords) / n
            places = tweet['location']
        if 'geo' in tweet and tweet['geo'] and\
           'coordinates' in tweet['geo'] and\
           tweet['geo']['coordinates']:
            lat, lon = tweet['geo']['coordinates']

        if (not lat or not lon) and 'object' and tweet and\
           'location' in tweet['object'] and tweet['object']['location'] and\
           'geo' in tweet['object']['location'] and tweet['object']['location']['geo']\
           and 'coordinates' in tweet['object']['location']['geo'] and\
           tweet['object']['location']['geo']['coordinates']:
            coords = tweet['object']['location']['geo']['coordinates'][0]
            n = float(len(coords))
            lon = sum(o for o, a in coords) / n
            lat = sum(a for o, a in coords) / n
            places = tweet['object']['location']

        if (not lat or not lon) and 'object' in tweet and\
           'geo' in tweet['object'] and\
           'coordinates' in tweet['object']['geo'] and\
           tweet['object']['geo']['coordinates']:
            lat, lon = tweet['object']['geo']['coordinates']

        if 'actor' in tweet:
            if 'location' in tweet['actor']:
                texts['user_location'] = tweet['actor']['location'].get('displayName','')
            if 'summary' in tweet['actor']:
                texts['user_description'] = tweet['actor']['summary']
        if 'body' in tweet and tweet['body']:
            texts['text'] = tweet['body'].strip()

        return lat, lon, places, texts

    @staticmethod
    def geo_normalize_datasift(datasift_tweet):
        """ Normalizes tweet from the datasift API
            in to geocodable chunks of information

        :param tweet: tweet payload
        :returns: lat, lon, places, texts
        """
        lat, lon, places, texts = None, None, {}, {}
        if 'twitter' in datasift_tweet and 'retweeted' in datasift_tweet['twitter']:
            texts['text'] = (datasift_tweet['twitter']['retweet']
                                           ['text'].strip())
            if 'location' in datasift_tweet['twitter']['retweeted']['user']:
                # store location of user who "retweeted"
                texts['rt_user_location'] = (datasift_tweet['twitter']
                                                           ['retweeted']
                                                           ['user']
                                                           ['location'])
            if 'description' in datasift_tweet['twitter']['retweeted']['user']:
                # store description of user who "retweeted"
                texts['rt_user_description'] = (datasift_tweet['twitter']
                                                              ['retweeted']
                                                              ['user']
                                                              ['description'])
            tweet = datasift_tweet['twitter']['retweeted']
            if 'location' in datasift_tweet['twitter']['retweet']['user']:
                texts['user_location'] = (datasift_tweet['twitter']['retweet']
                                                        ['user']
                                                        ['location'].strip())
            if 'description' in datasift_tweet['twitter']['retweet']['user']:
                texts['user_description'] = (datasift_tweet['twitter']
                                             ['retweet']['user']
                                             ['description'].strip())
        elif 'twitter' in datasift_tweet and 'text' in datasift_tweet['twitter']:
            texts['text'] = datasift_tweet['twitter']['text'].strip()
            if 'location' in datasift_tweet['twitter']['user']:
                texts['user_location'] = (datasift_tweet['twitter']
                                                        ['user']
                                                        ['location'].strip())
            if 'description' in datasift_tweet['twitter']['user']:
                texts['user_description'] = (datasift_tweet['twitter']
                                             ['user']['description'].strip())
            tweet = datasift_tweet['twitter']

        if 'geo' in tweet:
            lon = tweet['geo']['longitude']
            lat = tweet['geo']['latitude']
        if 'place' in tweet:
            places = tweet['place']

        return lat, lon, places, texts

    @staticmethod
    def normalize_places(places):
        """Extract city, admin1 and country from places
           json object of tweet
        :returns: (city, admin1, country)
        """
        city, admin1, country = [None] * 3
        if places:
            if 'place_type' in places:
                if (places['place_type'] == 'admin' and 'name' in places):
                    admin1 = places['name'].strip()
                elif places['place_type'] == 'city':
                    if 'name' in places:
                        city = places['name'].strip()
                    if 'full_name' in places:
                        ci_admin1 = places['full_name'].split(',')
                        if len(ci_admin1) > 0:
                            ci = ci_admin1[0]
                        if city is None and ci and len(ci.strip()) > 0:
                            city = ci.strip()
                        if len(ci_admin1) > 1:
                            adm1 = ci_admin1[-1]
                            if admin1 is None and len(adm1.strip()) > 0:
                                admin1 = adm1.strip()
                elif (places['place_type'] == 'poi'
                      or places['place_type'] == 'neighborhood'):
                    if 'full_name' in places:
                        ci_admin1 = places['full_name'].split(',')
                        if len(ci_admin1) > 1:
                            adm1 = ci_admin1[-1]
                            if admin1 is None and len(adm1.strip()) > 0:
                                admin1 = adm1.strip()
            elif 'name' in places:
                city = places['name'].strip()
            elif 'displayName' in places:
                ci_admin1 = places['displayName'].split(',')
                if len(ci_admin1) > 0:
                    ci = ci_admin1[0]
                if city is None and ci and len(ci.strip()) > 0:
                    city = ci.strip()
                if len(ci_admin1) > 1:
                    adm1 = ci_admin1[-1]
                    if admin1 is None and len(adm1.strip()) > 0:
                        admin1 = adm1.strip()

            if 'country' in places:
                country = places['country'].strip()
            elif 'country_code' in places:
                country = places['country_code'].strip()

        return city, admin1, country

    def _match_for_names(self, loc, city=None, admin1=None, country=None):
        if loc:
            if not country:
                country = self.co_reg.search(loc)
            if not city:
                city = self.ci_reg.search(loc)
                if city and country and city == country:
                    loc = (city and loc.replace(city, " ", 1)) or loc
                    city = None
                    city = self.ci_reg.search(loc)
                loc = (city and loc.replace(city, " ", 1)) or loc
            loc = (country and loc.replace(country, " ", 1)) or loc
            if not admin1:
                admin1 = self.admin1_reg.search(loc)
                loc = (admin1 and loc.replace(admin1, " ", 1)) or loc

        return loc, city, admin1, country

    def match_for_names(self, loc_str, city=None, admin1=None, country=None):
        """Extract city, admin1 and country names using regular expressions
           using some string - which primarily is the location
           string mentioned in twitter user's profile
        :param loc_str: location string, but could be any string containing
                        geocodable information
        :param city: name of city
        :param admin1: name of admin1
        :param country: name of country
        :returns: locstr, city, admin1, country
        NOTE: loc string returned is substring after removal
              of matched loc entities
        """
        loc = None
        if loc_str:
            loc = self.nstr(loc_str)
            loc, city, admin1, country = self._match_for_names(loc, city,
                                                               admin1,
                                                               country)

            for r, t in [('.', ''), ('-', ' '), (', ', ' '), (',', ' '),
                         ('#', ' ')]:
                loc = loc.replace(r, t)
            loc, city, admin1, country = self._match_for_names(loc, city,
                                                               admin1,
                                                               country)

        return loc, city, admin1, country

    def match_for_lat_lon(self, loc_str):
        """Extract city, admin1 and country information by finding 
           lat-lon from location string  using regular expressions
           It uses as input a location information
           containing string, which primarily is the location string mentioned
           in twitter user's profile
        :param loc_str: location string, but could be any string containing
                        geocodable information
        :returns: geo tuple 
        """
        if loc_str:
            try:
                lat, lon = LAT_LON_RE.search(loc_str).groups()
                if lat and lon:
                    return self.geo_from_lat_lon(float(lat), float(lon))
            except:
                return None

    def match_for_code(self, loc_str, city=None, admin1=None, country=None):
        """Extract city, admin1 and country names using regular expressions
           built using codes for country and admin1, which in turn are
           mapped to full name string. It uses as input a location information
           containing string, which primarily is the location string mentioned
           in twitter user's profile
        :param loc_str: location string, but could be any string containing
                        geocodable information
        :param city: name of city
        :param admin1: name of admin1
        :param country: name of country
        :returns: locstr, city, admin1, country
        NOTE: loc string returned is substring after removal
              of matched loc entities
        """
        loc = None
        if loc_str:
            loc = loc_str  # self.nstr(loc_str)
            for r, t in [('.', ''), ('-', ' '), (', ', ' '),
                         (',', ' '), ('#', ' ')]:
                loc = loc.replace(r, t)
            if not country:
                co_code = self.co_code_reg.search(loc)
                if co_code and co_code in self.co_code:
                    loc = loc.replace(co_code, " ", 1)
                    country = self.co_code[co_code]
            if not admin1:
                if not country:
                    admin1_code = self.admin1_code_reg2.search(loc)
                else:
                    admin1_code = self.admin1_code_reg1.search(loc)
                if admin1_code and admin1_code in self.admin_code:
                    co_a = self.admin_code[admin1_code]
                    co_a = [(self.nstr(c), a) for c, a in co_a
                            if self.nstr(a) in self.admin_name and
                            self.nstr(c) in self.admin_name[self.nstr(a)]]
                    if len(co_a) > 0:
                        x = None
                        if country:
                            co = self.nstr(country)
                            x = lambda i: i == co
                        elif len(co_a) == 1:
                            x = lambda i: True
                        else:
                            # to be safe if country is not known
                            # dont make guesses
                            x = lambda i: False
                        co_a = [(c, a) for c, a in co_a if x(c)]
                        if len(co_a) > 0:
                            c, a = co_a[0]
                            admin1 = a
                            if not country:
                                country = c
                    loc = (admin1_code
                           and loc.replace(admin1_code, " ", 1)) or loc

        return loc, city, admin1, country

    def resolve(self, city, admin1, country, res_level):
        """Resolves a given combination of city, admind1 and country.
           If a match is found returns all information for the location
           tuple, else resolves to specified granularity via resolution level
        :param city: name of city
        :param admin1: name of admin1
        :param country: name of countru
        :res_level: resolution level
        :returns: (city, country, admin1,
                  admin2, admin3, pop, lat, lon, id, pad)
        """
        m1, m2 = self.best_guess(city, admin1, country, res_level)
        if m1:
            return m1[0]
        elif m2:
            return m2[0]
        else:
            return None

    def _normalize_payload(self, tweet):
        """Normalizes the tweet to extract all geocode-able information
        :param tweet: json object of tweet
        :returns: lat, lon, places, texts
        """
        lat, lon, places, texts, enr = None, None, {}, {}, {}
        if 'interaction' in tweet and 'twitter' in tweet:
            lat, lon, places, texts = self.geo_normalize_datasift(tweet)
        elif 'gnip' in tweet:
            lat, lon, places, texts = self.geo_normalize_gnip(tweet)
        else:
            lat, lon, places, texts = self.geo_normalize_public(tweet)
        if 'BasisLocationEnrichment' in tweet:
            enr = tweet['BasisLocationEnrichment']
        return lat, lon, places, texts, enr

    def _geos_from_twitter_place(self, twitter_place):
        ci, a, co = self.normalize_places(twitter_place)
        ci = self.nstr(ci) if ci else ci
        co = self.nstr(co) if co else co
        a = self.nstr(a) if a else a
        return self.resolve(ci, a, co, RESOLUTION_LEVEL.country)

    def _geos_from_tweet_text(self, tweet_text):
        #for r, t in [('Una', ''), ('una', ''), ('mato', ''), ('Mato', '')]:
        #    tweet_text = tweet_text.replace(r, t)
        loc_str, ci, a, co = self.match_for_names(tweet_text)
        return self.resolve(ci, a, co, RESOLUTION_LEVEL.country)

    def _best_alt_match(self, alt_names, loc_str):
        best_alt_name, best_match_score = None, 0.0
        for a in alt_names:
            r = FUZZY_MATCH(a['name'], loc_str)
            if r > best_match_score:
                best_match_score = r
                best_alt_name = a['name']
        return best_alt_name, best_match_score


    def _contains_country(self, basis_rni_matches):
        countries = []
        for m in basis_rni_matches:
            if 'source' in m and m['source'] == 'Country_list' and m['name'] and\
               m['confidence'] > 0.0:
                countries.append((m['name'], m['confidence']))
            elif m['name'] and m['name'] in NON_MENA_COUNTRIES:
                countries.append((m['name'], m['confidence']))
        return sorted(countries, key=lambda x:x[1], reverse=True)
            
    def contains_country(self, basis_rni_matches):
        has_country = False
        countries = self._contains_country(basis_rni_matches)
        if countries:
            has_country = True
            countries = [c for c, confidence in countries if c in MENA_COUNTRIES]
            if countries:
                for co in countries:
                    return has_country, self.resolve(None, None, co, RESOLUTION_LEVEL.country) 
        return has_country, None

    def _best_match(self, locs, loc_str, fuzzy_threshold=0.9):
        best_match, best_score, entity = None, fuzzy_threshold, None
        
        for loc in locs:
            bm, bs = self._best_alt_match(loc['alt_names'], loc_str)
            if bs > best_score:
                best_match = bm
                best_score = bs
                entity = {k:v for k,v in loc.items() if k != 'alt_names'}
        return best_match, best_score, entity

    def _geo_normalize(self, lat, lon, places, texts, enr, policies):
        """Determine geo-location for the tweet. Using the normalized
           from geocode-able information from tweet payload. And then performs
           various passes to extract relevant information which is then
           best-guess'ed to a appropriate match
        :param lat: GPS lat coordinate
        :param lon: GPS lon coordinate
        :param places: Twitter Places object
        :param texts: Tweet text information, user-profile text
        :param policies: multipass policy 
        :returns: (city, country, admin1,
                  admin2, admin3, pop, lat, lon, id, pad, source)
                  NOTE: source indicates which part of the tweet
                        particular geocoded information was extracted
                        from
        """
        geos = None
        best_geos = None
        best_policy = [ ]
        for policy in policies:
            if policy == 'geo_place':
                geos = self._geos_from_twitter_place(places)
                if self.debug and not geos:
                    self.log.info("Could not resolve place: {0}".format(
                                  places))
            elif policy == 'geo_coordinates':
                geos = self.geo_from_lat_lon(lat, lon)
                if self.debug and not geos:
                    self.log.info("lat-lon couldn't be resolved {0}".format(
                                  (lat, lon)))
            elif policy == 'user_location':
                if 'user_location' in texts and texts['user_location']:
                    geos = self.match_for_lat_lon(texts['user_location'])
                    if not geos:
                        loc_str, ci, a, co = self.match_for_names(
                            texts['user_location'])
                        loc_str, ci, a, co = self.match_for_code(
                            texts['user_location'], ci, a, co)
                        geos = self.resolve(ci, a, co, RESOLUTION_LEVEL.country)
            elif policy == 'user_location_rnt' and enr:
                candidate_locations = []
                if 'RNT_Translation' in enr and enr['RNT_Translation']:
                    locations = sorted(enr['RNT_Translation'],
                                       key=lambda x:x['translation']['confidence'], reverse=True)
                    candidate_locations += [nstr(l['translation']['name'], False) for l in locations if l['translation']['name']]
                    best_loc = None
                    for loc in candidate_locations:
                        loc_str, ci, a, co = self.match_for_names(loc)
                        geos = self.resolve(ci, a, co, RESOLUTION_LEVEL.country)
                        # iterative improvement of geocode
                        if geos:
                            if best_loc:
                                if best_loc[1] == geos[1]:
                                    if geos[2] and (best_loc[2] is None or best_loc[2] == geos[2]):
                                        best_policy.append(loc)
                                        best_loc = copy.deepcopy(geos)
                                        if geos[0]:
                                            break
                            elif geos[0]:
                                best_policy.append(loc)
                                best_loc = copy.deepcopy(geos)
                                break
                            else:
                                best_policy.append(loc)
                                best_loc = copy.deepcopy(geos)

                    geos = best_loc
            elif policy == 'user_location_rni' and enr:
                if 'RNI_NameMatches' in enr and enr['RNI_NameMatches'] and\
                   'RNT_Translation' in enr and enr['RNT_Translation']:
                    user_loc_str = sorted(enr['RNT_Translation'],
                                       key=lambda x:x['translation']['confidence'],
                                       reverse=True)[0]['translation']['name']
                    locations = []
                    for m in enr['RNI_NameMatches']:
                         if 'RNImatch' in m and m['RNImatch'] and\
                            m['RNImatch'][0]['confidence'] > 0.0:
                             locations.append(m['RNImatch'][0])
                    if locations:
                        has_co, geos = self.contains_country(locations)
                        if not has_co:
                            best_match, best_score, entity = self._best_match(locations, user_loc_str)
                            if entity:
                                if entity['name'] in MENA_COUNTRIES:
                                    geos = self.resolve(None, None, entity['name'], RESOLUTION_LEVEL.country)
                                elif best_match in MENA_COUNTRIES:
                                    geos = self.resolve(None, None, best_match, RESOLUTION_LEVEL.country)
                                else:
                                    coords = entity['coordinates']
                                    geos = self.geo_from_lat_lon(coords['latitude'], coords['longitude'], 110)
                                if geos:
                                    best_policy.append(":".join([best_match, user_loc_str, str(best_score)]))


            # iterative improvement of geocode
            if geos:
                if best_geos:
                    if best_geos[1] == geos[1]:
                        if geos[2] and (best_geos[2] is None or best_geos[2] == geos[2]):
                            best_policy.append(policy)
                            best_geos = copy.deepcopy(geos)
                            if geos[0]:
                                break
                elif geos[0]:
                    best_policy.append(policy)
                    best_geos = copy.deepcopy(geos)
                    break
                else:
                    best_policy.append(policy)
                    best_geos = copy.deepcopy(geos)

        if best_geos is None:
            return (None, None, None, None, None,
                    None, None, None, None, 0, None)
        else:
            return best_geos + (",".join(best_policy),)

    def geo_normalize(self, tweet, usecache=True, policy=None):
        if "embersGeoCode" in tweet:
            if ("geocode_version" in tweet["embersGeoCode"] and usecache and
               tweet["embersGeoCode"]["geocode_version"] == self.__version__):
                cache = tweet["embersGeoCode"]
                return tuple(cache[k] for k in ["city", "country", "admin1",
                                                "admin2", "admin3", "pop",
                                                "latitude", "longitude",
                                                "id", "pad", "source"])
        lat, lon, places, texts, enr = self._normalize_payload(tweet)
        if policy:
            return self._geo_normalize(lat, lon, places, texts, enr, policy)
        else:
            return self._geo_normalize(lat, lon, places, texts, enr, self.priority_policy)

    def annotate(self, tweet, usecache=True, keyname='embersGeoCode', policy=None):
        """Annotate given tweet with geocoding. It will modify the
           dictionary to include the results of geo_normalize
        :param tweet: json object of tweet
        :returns: anotated tweet
        """
        geos = self.geo_normalize(tweet, usecache, policy)
        tweet[keyname] = {
            "geocode_version": self.__version__,
            "city": geos[0],
            "country": geos[1],
            "admin1": geos[2],
            "admin2": geos[3],
            "admin3": geos[4],
            "pop": geos[5],
            "latitude": geos[6],
            "longitude": geos[7],
            "id": geos[8],
            "pad": geos[9],
            "source": geos[10]
        }
        return tweet
