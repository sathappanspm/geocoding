#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

import pdb
from geoutils.gazetteer_mod import GeoNames
from geoutils.dbManager import SQLiteWrapper, ESWrapper
#from geoutils.gazetteer import GeoNames
from geoutils import LocationDistribution, encode, CountryDB, GeoPoint
import logging
from geocode import BaseGeo
import re


emoji_pattern = re.compile("["
                u"\U0001F600-\U0001F64F"  # emoticons
                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                u"\u2070-\u303f"
                u"0-9"
                "]+|\\buniversity\\b|\\bcity\\b", flags=re.UNICODE)

#ENG_LATIN_RE = re.compile(u"[\u0020-\u024F]")
ENG_LATIN_RE = re.compile(u"[\u0021-\u007F\u00A1-\u024F]")
#RE_SPLIT = re.compile('\.|,|\-|_')
RE_SPLIT = re.compile(',|\-|_')
logging.basicConfig(filename='geocode_twitter.log', level=logging.INFO)
log = logging.getLogger("twittergeocoder")
eslog = logging.getLogger('elasticsearch')
eslog.setLevel(logging.ERROR)
log.setLevel(logging.INFO)
urlib3logger = logging.getLogger('urllib3')
urlib3logger.setLevel(logging.ERROR)

def get_translated_text(basisObj):
    """
    return the concatenated string of translated Named Entities if present
    """
    concatenated_text = []
    for e in basisObj.get('entities', []):
        if 'translation' in e:
            concatenated_text.append(e['translation'])
    return " ".join(concatenated_text)


class TweetGeocoder(BaseGeo):
    def __init__(self, db, min_popln=5000, min_length=1, weightage=None):
        self.gazetteer = GeoNames(db)
        self.min_popln = min_popln
        self.min_length = min_length
        if weightage is None:
            self.weightage = {'coordinates': 1.0,
                              'places': 0.92,
                              'rt_user_description': 0.167,
                              'rt_user_location': 0.57,
                              'text': 0.07,
                              'user_description': 0.145,
                              'user_location': 0.95}
        else:
            self.weightage = weightage

        self.analyzer = {#"text": "tokenLengthAnalyzer",
                         "user_location": "standard",
                         "rt_user_location": None}
        #self.analyzer = {}

    def _geo_lat_lon(self, lat, lon, min_popln):
        return self.gazetteer.db.near_geo([lon, lat], size=1, min_popln=min_popln)

    def _geo_text(self, texts, min_popln):
        locations = {}
        for t in texts:
            wrds = RE_SPLIT.split(emoji_pattern.sub("", texts[t].lower()))
            ts = ([tmp.strip() for tmp in wrds]) #if len(tmp.strip()) > 3])
            t_analyzer = self.analyzer.get(t, "tokenLengthAnalyzer")
            if not ENG_LATIN_RE.search(texts[t]):
                t_analyzer = "standard"
            #if len(ts) == 1:
            #    txt = " ".join(ts)
            #    ts = None
            #txt = " ".join([tmp for tmp in texts[t].split() if len(tmp) > 3]).lower()
            #if "-" in txt:
            #    ts = [tmp.strip() for tmp in txt.split("-")]
            #elif "," in txt:
            #    ts = [tmp.strip() for tmp in txt.split(",")]
            #elif "." in txt:
            #    ts = [tmp.strip() for tmp in txt.split(".")]
            #print ts, wrds
            if len(ts) > 1:
                ts.append(texts[t].lower().strip())
                gpts = self.get_geoPoints_intersection({tmp:
                                                        self.gazetteer.query(tmp,
                                                                             analyzer=t_analyzer,
                                                                                  min_popln=min_popln)
                                                        for tmp in ts}, min_popln)
            else:
                txt = " ".join(ts) if ts else texts[t].lower().strip()
                gpts = self.gazetteer.query(txt, analyzer=t_analyzer, min_popln=min_popln)
                #print txt, gpts
            locations[t] = LocationDistribution(gpts)
            locations[t].frequency = 1
        #return gpts
        return locations

    def _subgeo(self, gps, selcountry):
        """
        check if g falls in hierarchy of any of the pts
        """
        #cogps = {gp: [k for k in gps[gp] if k.country in selcountry] for gp in gps}
        #gps = cogps
        cogps = {}
        #adkeys = {gp: {(adm.country+adm.admin1): adm for adm in cogps[gp] if adm.ltype=="admin1"} for gp in cogps}
        hashmap = {}
        #for gp in cogps:
        #    tmp = {}
        #    for pt in cogps[gp]:
        #        pstr = pt.country + pt.admin1
        #        if pstr in tmp:
        #            tmp[pstr].append(pt)
        #        else:
        #            tmp[pstr] = [pt]

        #    hashmap[gp] = tmp

        for gp in gps:
            tmp = {}
            pts = []
            for pt in gps[gp]:
                if pt.country in selcountry:
                    if pt.ltype == "country":
                        tmp = {}
                        break
                    pstr = pt.country + pt.admin1
                    pts.append(pt)
                    if pstr in tmp:
                        tmp[pstr].append(pt)
                    else:
                        tmp[pstr] = [pt]

            if tmp:
                cogps[gp] = pts
                hashmap[gp] = tmp

        adkeys = {gp: {(adm.country+adm.admin1): adm for adm in cogps[gp] if adm.ltype=="admin1"} for gp in cogps}
        fnlgps = {}
        for gp in cogps:
            ng = fnlgps.get(gp, [])
            if adkeys[gp]:
                pts = cogps.viewkeys() - (gp,)
                admins = adkeys[gp].viewkeys()
                for pt in pts:
                    ix = admins & (hashmap[pt].viewkeys())
                    if ix:
                        ng.extend((adkeys[gp][x] for x in ix))
                        ig = fnlgps.get(pt, [])
                        ig.extend((y for x in ix for y in hashmap[pt][x]))
                        fnlgps[pt] = ig

            if not ng:
                ng = cogps[gp]

            fnlgps[gp] = ng

        return fnlgps

    def get_geoPoints_intersection(self, gps, min_popln):
        selcountry = None
        sel_admin1 = None
        try:
            selcountry = set.intersection(*[set([l.country for l in gps[name]])
                                            for name in gps])
        except:
            selcountry = None

        if not selcountry:
            #return self.gazetteer._get_loc_confidence([l.__dict__ for name in gps for l in gps[name]], min_popln)
            return self.gazetteer._get_loc_confidence([l for name in gps for l in gps[name]], min_popln)

        #filtered_gps = [set([encode('/'.join([l.country, l.admin1, ""])) for
        #                     l in gps[name] if l.country in selcountry])
        #                for name in gps if (len(gps[name]) != 1 and gps[name][0].ltype != 'country')]

        #return gps

        gps = self._subgeo(gps, selcountry)
        #filtered_gps = [set(['/'.join([l.country, l.admin1, ""]) for
        #            l in gps[name]]) for name in gps if (len(gps[name]) != 1 and gps[name][0].ltype != 'country')]

        #filtered_gps = [set(['/'.join([l.country, l.admin1, ""]) for
        #            l in gps[name] if l.ltype != "country"]) for name in gps ] #if len(gps[name]) > 1]

        filtered_gps = []
        for nm in gps:
            es = set()
            for pt in gps[nm]:
                if pt.ltype == "country":
                    es = set()
                    break
                    #continue

                es.add("/".join([pt.country, pt.admin1, ""]))
            if es:
                filtered_gps.append(es)
                #fgps[nm] = es


        if filtered_gps:
            sel_admin1 = set.intersection(*filtered_gps)
            #sel_admin1 = set.intersection(*filtered_gps.values())

            #try:
            #    #sel_admin1 = set.intersection(*[s for s in filtered_gps if s])
            #except:
            #    #print filtered_gps, selcountry, gps
            #    sel_admin1 = None

        if not sel_admin1:
            #return self.gazetteer._get_loc_confidence([l.__dict__ for name in gps for l in gps[name]], min_popln)
            return self.gazetteer._get_loc_confidence([l for name in gps for l in gps[name]], min_popln)

        #ns = [gp.__dict__ for name in gps for gp in gps[name]
        #      if ("/".join([gp.country, gp.admin1, ""])) in sel_admin1]

        #ns = [gp for name in gps for gp in gps[name]
        #      if ("/".join([gp.country, gp.admin1, ""])) in sel_admin1]
        ns = []
        admins = []

        for nm in gps:
            es = []
            flag = False
            for pt in gps[nm]:
                if pt.ltype == "country":
                    flag = True
                elif ("/".join([pt.country, pt.admin1, ""])) in sel_admin1:
                    if flag or pt.ltype == "admin1":
                        flag = True
                        admins.append(pt)
                    else:
                        es.append(pt)
            if not flag:
                ns += es

        if not ns:
            ns = admins
        #if len(ns) > 1:
        #    #tmp = [l for l in ns if l.ltype == 'city']
        #    if tmp != []:
        #        ns = tmp

        return self.gazetteer._get_loc_confidence(ns, min_popln)

    def geocode(self, doc=None, min_popln=None):
        if min_popln is None:
            min_popln = self.min_popln

        lat, lon, places, text = self._geo_normalize_gnip(doc)
        return self._geocode(lat, lon, places, text, min_popln)

    def _geocode(self, lat, lon, places, text, min_popln):
        results = {}
        if lat and lon:
            results['coordinates'] = LocationDistribution(self._geo_lat_lon(lat, lon, min_popln))
            results['coordinates'].frequency = 1

        if places:
            results['places'] = LocationDistribution(self._geo_places(places, min_popln))
            results['places'].frequency = 1

        results.update(self._geo_text(text, min_popln))

        scores = self.score(results)
        custom_max = lambda x: max(x.viewvalues(),
                                   key=lambda y: y['score'])
        lrank = self.get_locRanks(scores, results)
        lmap = self.rescale({l: custom_max(lrank[l]) for l in lrank if not lrank[l] == {}})
        #pdb.set_trace()
        total_weight = sum([self.weightage[key] for key in (self.weightage.viewkeys() - lmap)])
        return lmap, max(lmap.items(), key=lambda x: x[1]['score'] * self.weightage[x[0]] / total_weight)[1]['geo_point'] if scores else {}

    def evaluate(self, doc):
        """
        test performance
        """
        try:
            lat, lon, places, text = self._geo_normalize_gnip(doc)
            true_geo = self._geo_lat_lon(lat, lon, self.min_popln)[0]
            #text.pop("text", '')
            #text.pop("user_description", '')
            #txt = {'user_location': text.pop('rt_user_description', '')}
            #lmap, gp = self._geocode(None, None, places, text, self.min_popln)
            lmap, gp = self._geocode(lat, lon, places, text, self.min_popln)
            #lmap, gp= {}, {}
        except Exception as e:
            log.exception("unable to geocode:{}".format(str(e)))
            lmap, gp = {}, {}

        doc['embersGeoCode'] = gp
        doc["location_distribution"] = lmap
        doc['true_geo'] = true_geo.__dict__
        return doc

    def rescale(self, locs):
        return locs
        popln_sum = sum([l['score'] for l in locs.viewvalues()])
        for l in locs:
            locs[l]['score'] = (locs[l]['score']) / popln_sum

        return locs

    def _geo_normalize_gnip(self, tweet):
        """ Normalizes tweet from the GNIP API
            in to geocodable chunks of information

        :param tweet: tweet payload
        :returns: lat, lon, places, texts
        """
        lon, lat, places, texts = None, None, {}, {}

        if('location' in tweet.get('object', {}) and tweet['object']['location'] and
           'geo' in tweet['object']['location'] and tweet['object']['location']['geo']
           and 'coordinates' in tweet['object']['location']['geo'] and
           tweet['object']['location']['geo']['coordinates']):
            coords = tweet['object']['location']['geo']['coordinates'][0]
            n = float(len(coords))
            lon = sum(o for o, a in coords) / n
            lat = sum(a for o, a in coords) / n

        if('profileLocations' in tweet.get('gnip', {}) and tweet['gnip']['profileLocations'] and
           'geo' in tweet['gnip']['profileLocations'][0] and
           tweet['gnip']['profileLocations'][0]['geo']
           and 'coordinates' in tweet['gnip']['profileLocations'][0]['geo'] and
           tweet['gnip']['profileLocations'][0]['geo']['coordinates']):
            coords = tweet['gnip']['profileLocations'][0]['geo']['coordinates']
            lon, lat = coords

        if 'location' in tweet and tweet['location'] and\
           'geo' in tweet['location'] and tweet['location']['geo']\
           and 'coordinates' in tweet['location']['geo'] and\
           tweet['location']['geo']['coordinates']:

            if (not lat or not lon):
                coords = tweet['location']['geo']['coordinates'][0]
                n = float(len(coords))
                lon = sum(o for o, a in coords) / n
                lat = sum(a for o, a in coords) / n
            places = tweet['location']

        if 'geo' in tweet and tweet['geo'] and\
           'coordinates' in tweet['geo'] and\
           tweet['geo']['coordinates']:
            lat, lon = tweet['geo']['coordinates']

        if((not lat or not lon) and 'object' in tweet and
           'location' in tweet['object'] and tweet['object']['location'] and
           'geo' in tweet['object']['location'] and tweet['object']['location']['geo']
           and 'coordinates' in tweet['object']['location']['geo'] and
           tweet['object']['location']['geo']['coordinates']):
            coords = tweet['object']['location']['geo']['coordinates'][0]
            n = float(len(coords))
            lon = sum(o for o, a in coords) / n
            lat = sum(a for o, a in coords) / n
            places = tweet['object']['location']

        if((not lat or not lon) and 'object' in tweet and
           'geo' in tweet['object'] and
           'coordinates' in tweet['object']['geo'] and
           tweet['object']['geo']['coordinates']):
            lat, lon = tweet['object']['geo']['coordinates']

        if 'actor' in tweet:
            if 'location' in tweet['actor']:
                actor_loc = tweet['actor']['location']
                texts['user_location'] = actor_loc.get('displayName', '')
                trans_uLoc = get_translated_text(actor_loc.get('displayNameEnriched', {}))
                if trans_uLoc != "":
                    texts['user_location'] = trans_uLoc
            if 'summary' in tweet['actor'] and tweet['actor']['summary'] is not None:
                texts['user_description'] = tweet['actor']['summary']
                trans_uDesc = get_translated_text(tweet['actor'].get('summaryEnriched', {}))
                if trans_uDesc != "":
                    texts['user_description'] = trans_uDesc

        if 'verb' in tweet and tweet['verb'] == 'share':
            rtobj = tweet['object']['actor']
            if 'location' in rtobj:
                texts['rt_user_location'] = rtobj['location'].get('displayName', '')
                trans_rtloc = get_translated_text(rtobj['location'].get('displayNameEnriched', {}))
                if trans_rtloc != '':
                    texts['rt_user_location'] = trans_rtloc
            if 'summary' in rtobj and rtobj['summary'] is not None:
                texts['rt_user_description'] = rtobj['summary']
                trans_userDescription = get_translated_text(rtobj.get('summaryEnriched', {}))
                if trans_userDescription != '':
                    texts['rt_user_description'] = trans_userDescription

        texts['text'] = tweet['body'].strip()
        trans_text = get_translated_text(tweet.get('BasisEnrichment', {}))
        if trans_text != '':
            texts['text'] = trans_text

        ### at times text fields have all characters separated by space
        ### for example, 'C A N A D A'
        for t in texts:
            if (len(texts[t].split()) / float(len(texts[t]))) >= 0.5:
                texts[t] = "".join(texts[t].split())

        return lat, lon, places, texts

    def _geo_places(self, places, min_popln):
        """Extract city, admin1 and country from places
           json object of tweet
        :returns: (city, admin1, country)
        """
        loclist = {}
        #kwargs = {}
        if 'country' in places:
            cstr = places['country'].strip()
            country = self.gazetteer._querycountry(cstr)
            if country == []:
                country = self.gazetteer.query(cstr.lower(), featureCode='pcli')

            loclist[cstr] = country
            #cc2 = country.get("ISO", country.get('countryCode')).lower()
            #kwargs = {'countryCode': cc2}

        if 'twitter_country_code' in places:
            loclist[places['twitter_country_code']] = CountryDB.fromISO(places['twitter_country_code'])
            if loclist[places['twitter_country_code']]:
                loclist[places['twitter_country_code']] = [GeoPoint(**loclist[places['twitter_country_code']])]

        if 'name' in places:
            loclist[places['name']] = self.gazetteer.query(places['name'].lower(),
                                                           min_popln=min_popln,
                                                           analyzer='standard')

        if 'full_name' in places:
            for tmp in RE_SPLIT.split(places['full_name']):
                if tmp not in loclist:
                    loclist[tmp] = self.gazetteer.query(tmp, min_popln=min_popln,
                                                        analyzer='standard')

        if 'displayName' in places:
            for tmp in RE_SPLIT.split(places['displayName']):
                if tmp not in loclist:
                    loclist[tmp] = self.gazetteer.query(tmp, min_popln=min_popln,
                                                        analyzer='standard')

        return self.get_geoPoints_intersection(loclist, min_popln)


if __name__ == "__main__":
    #db = SQLiteWrapper(dbpath="./geoutils/GN_dump_20160407.sql")
    db = ESWrapper(index_name="geonames", doc_type="places")
    geo = BaseGeo(db, min_length=3)
