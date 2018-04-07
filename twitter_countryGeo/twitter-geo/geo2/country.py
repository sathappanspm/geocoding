#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'
__version__ = '0.0.2'

import os
import hashlib
from esmre import esm
from fiona import collection
from rtree import index
from shapely.geometry import shape, Point
import csv
import pdb
import json


def encode(s, coding="utf-8"):
    try:
        return s.encode('utf-8')
    except:
        return s


def decode(s, coding='utf-8'):
    try:
        return s.decode('utf-8')
    except:
        return s


def isempty(s):
    """
    return if string is empty
    """
    return s in (None, "", "-")


class DataObject(dict):
    def set_version(self, datafile):
        with open(datafile) as infile:
            self.version = hashlib.sha512(infile.read()).hexdigest()


class Twitter_TZs(DataObject):
    def __init__(self):
        tz_datafile = os.path.join(os.path.dirname(__file__),
                                   "timezones.tsv")
        self.set_version(tz_datafile)
        with open(tz_datafile) as infile:
            data = [l.decode("UTF8").strip("\r\n").split("\t") for l in infile]
        data = ((tz, cc if cc else None) for tz, cc in data)
        super(Twitter_TZs, self).__init__(data)

    #def __getitem__(self, key):
    #    if isempty(key):
    #        return None
    #    try:
    #        return self[key]
    #    except KeyError:
    #        print "timezone-{} Not Found".format(key)
    #    return None


class CountryNames(DataObject):
    def __init__(self):
        # cn_datafile = os.path.join(os.path.dirname(__file__),
        #                            "multilingualNames.csv")
        # self.set_version(cn_datafile)
        # with open(cn_datafile) as infile:
        #     data = {}
        #     infile.next()
        #     names = ['langCode', 'langName', 'iso2', 'iso3', 'isoNumeric', 'cname']
        #     reader = csv.DictReader(infile, fieldnames=names)
        #     for l in reader:
        #         cname = encode(l['cname'].lower())
        #         if len(cname) < 4:
        #             continue
        #         if cname not in data:
        #             data[cname] = {
        #                 'langCode': [l['langCode']],
        #                 "langName": [l['langName']],
        #                 'iso2': l['iso2'],
        #                 'iso3': l['iso3'],
        #                 'isoNumeric': l['isoNumeric'],
        #                 'cname': cname
        #             }
        #         else:
        #             data[cname]['langCode'].append(l['langCode'])
        #             data[cname]['langName'].append(l['langName'])
        # self.esmindex = esm.Index()
        # [self.esmindex.enter(i) for i in data.viewkeys()]
        # self.esmindex.fix()
        # self.data = data

        cn_datafile = os.path.join(os.path.dirname(__file__),
                                    "country_multilingual.csv")
        self.set_version(cn_datafile)
        with open(cn_datafile) as infile:
            data = {}
            infile.next()
            names = ['iso2', 'cname', 'langCode']
            reader = csv.DictReader(infile, fieldnames=names)
            for l in reader:
                cname = encode(l['cname'].lower())
                if len(cname) < 4:
                    continue
                if cname not in data:
                    data[cname] = {
                        'langCode': [l['langCode']],
                        'iso2': l['iso2'],
                        'cname': cname
                    }
                else:
                    data[cname]['langCode'].append(l['langCode'])
        self.esmindex = esm.Index()
        [self.esmindex.enter(i) for i in data.viewkeys()]
        self.esmindex.fix()
        self.data = data

    def query(self, text):
        if text is None:
            return []
        try:
            result = self.esmindex.query(encode(text).lower())
            if result != []:
                result = [self.data[rstr[1]]['iso2'] for rstr in result]
            else:
                result = []
        except Exception, e:
            raise e
            result = []

        return result


class GeoCoordinates(DataObject):
    def __init__(self):
        shape_file = os.path.join(os.path.dirname(__file__),
                                  "TM_WORLD_BORDERS-0.3.shp")
        self.data = list(collection(shape_file))
        self.tree_idx = index.Rtree()
        self.jsondata = {}
        for s in self.data:
            s['shape'] = shape(s['geometry'])
            ccode = s['properties']['ISO2']
            self.jsondata[ccode] = s['properties']
            del(s['geometry'])
            self.tree_idx.insert(int(s['id']), s['shape'].bounds, None)

        self.set_version(shape_file)
        self.regionMap = {2: {'continent': 'Africa',
                              'subregions': {14: 'Eastern Africa',
                                             17: 'Middle Africa',
                                             18: 'Southern Africa',
                                             11: 'Western Africa',
                                             15: 'Northern Africa'
                                             }
                              },
                          19: {'continent': 'Americas',
                               'subregions': {13: 'Central America',
                                              29: 'Carribean',
                                              5: 'South America',
                                              21: 'Northern America'
                                              },
                               },
                          142: {'continent': 'Asia',
                                'subregions': {143: 'Central Asia',
                                               30: 'Eastern Asia',
                                               34: 'Southern Asia',
                                               35: 'South-Eastern Asia',
                                               145: 'Western Asia'
                                               }
                                },
                          150: {'continent': 'Europe',
                                'subregions': {151: 'Eastern Europe',
                                               154: 'Northern Europe',
                                               39: 'Southern Europe',
                                               155: 'Western Europe'
                                               }
                                },
                          9: {'continent': 'Oceania',
                              'subregions': {53: 'Austrailia and New Zealand',
                                             54: 'Melanesia',
                                             57: 'Micronesia',
                                             61: 'polynesia'
                                             }
                              }
                          }

    def query(self, lat, lon):
        """
        return country given latitude and longitude
        """
        val = {'iso3': None, 'iso2': None, 'name': None}
        try:
            pt = Point((lon, lat))
            for j in self.tree_idx.intersection((lon, lat)):
                if pt.within(self.data[j]['shape']) is True:
                    res = self.data[j]['properties']
                    val = {'iso2': res['ISO2'], 'iso3': res['ISO3'], 'name': res['NAME']}
                    break

        except Exception:
            pass
        return val

    def get_region(self, cCodes):
        """
        Return region given country code
        """
        try:
            res = {'iso2': [], 'iso3': [], 'country': [],
                    'region': [], 'continent': []}
            for iso2Code in cCodes:
                r = self.jsondata[iso2Code]
                res['iso2'].append(r['ISO2'])
                res['iso3'].append(r['ISO3'])
                res['country'].append(r['NAME'])
                rCode = r['REGION']
                res['continent'].append(self.regionMap[rCode]['continent'])
                res['region'].append(self.regionMap[rCode]['subregions'][r['SUBREGION']])
        except Exception, e:
            pass

        return res


class GeoCountry(object):
    def __init__(self, embersRegions=None):
        self.twit_tz = Twitter_TZs()
        self.georesolve = GeoCoordinates()
        self.country_names = CountryNames()
        self.version = __version__ + "-"
        self.version += hashlib.md5(self.twit_tz.version
                                    + self.georesolve.version
                                    + self.country_names.version).hexdigest()
        if embersRegions is None:
            with open(os.path.join(os.path.dirname(__file__),
                      'embersCountries.conf')) as infile:
                self.embersregion = {decode(country): decode(region) for region, cnames in
                            json.load(infile).items() for country in cnames}
        else:
            self.embersregion = embersRegions

    def annotate(self, data):
        geo_obj = self.getcountry(data)
        # If the tweet is a retweet, check data['object'] also
        if data.get("verb", "post") == 'share':
            geo_obj_retweet = self.getcountry(data['object'])
        else:
            geo_obj_retweet = {"country": [], "region": [],
                               'continent': []}

        data['embersGeoCodeCountry'] = {
            "country": geo_obj_retweet['country'] + geo_obj['country'],
            "region": geo_obj_retweet["region"] + geo_obj['region'],
            "continent": geo_obj_retweet["continent"] + geo_obj['continent'],
            "version": self.version
        }
        data['embersGeoCodeCountry']['embersRegion'] = []
        if self.embersregion is not None:
            for co in data['embersGeoCodeCountry']['country']:
                eregion = None if co not in self.embersregion else self.embersregion[co]
                data['embersGeoCodeCountry']['embersRegion'].append(eregion)
        return data

    def getcountry(self, data):
        # Find country from Timezone
        tz = data["actor"]["twitterTimeZone"]
        tz_country = []
        if not isempty(tz):
            try:
                tz_country = [self.twit_tz[tz]]
            except KeyError:
                pass
                #print 'timezone-{} not in list'.format(tz)

        # Find country from tweet text
        text_country = self.country_names.query(data['body'])

        # Find country from description
        summary_country = []
        if 'summary' in data['actor']:
            summary_country = self.country_names.query(data['actor']['summary'])

        # Find country from location description
        loc_description_country = []
        if "location" in data["actor"]:  # ["location"]:
            location = data["actor"]["location"]["displayName"].lower()
            loc_description_country = self.country_names.query(location)

        loc_code_country = []
        if "location" in data:
            ccode = data["location"]["country_code"]
            if len(ccode) == 2:
                loc_code_country = self.georesolve.get_region(ccode.upper())['iso2']
            else:
                loc_code_country = self.country_names.query(ccode.lower())

        # resolve body_country , preference given to country name in text
        body_country = (text_country + loc_description_country +
                        summary_country + loc_code_country)

        geoPoint_country = []
        if 'geo' in data and 'coordinates' in data['geo']:
            lat, lon = data['geo']['coordinates']
            geoPoint_country = self.georesolve.query(lat, lon)['iso2']
            geoPoint_country = [] if geoPoint_country is None else [geoPoint_country]

        resolved_country = geoPoint_country + body_country + tz_country

        if resolved_country != []:
            geo_obj = self.georesolve.get_region(resolved_country)
        else:
            geo_obj = {'country': [], 'region': [], 'continent': []}

        return geo_obj


def main(args):
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-q", "--quiet", action="store_false", dest="verbose",
                        help="don't print status messages to stdout")
    parser.add_argument('--version', action='version',
                        version='%(prog)s ' + __version__)
    parser.add_argument("-s", "--summary", action="store_true")
    parser.add_argument("-c", "--conf", type=str, help="location of conf file")
    parser.add_argument('file')
    options = parser.parse_args()

    from collections import Counter
    import sys
    if options.conf:
        with open(options.conf) as cfile:
            embersregion = {decode(country): decode(region) for region, cnames in
                            json.load(cfile).items() for country in cnames}
    else:
        embersregion = None

    geoc = GeoCountry(embersRegions=embersregion)
    datas = (json.loads(line) for line in open(options.file))
    coded = (geoc.annotate(data) for data in datas)

    if options.summary:
        ccs = ((data["embersGeoCodeCountry"]["country"],
                data["embersGeoCodeCountry"]["embersRegion"])
               for data in coded)
        counter = Counter(ccs)
        for k in counter:
            print k, counter[k]
    else:
        try:
            for data in coded:
                eregions = data["embersGeoCodeCountry"]["embersRegion"]
                if 'LA' in eregions or 'MENA' in eregions:
                    print >> sys.stdout, json.dumps(data, ensure_ascii=False).encode('utf-8')
        except UnicodeDecodeError, e:
            print str(e)
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv))
