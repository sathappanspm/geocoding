#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from workerpool import WorkerPool
from geoutils.gazetteer_mod import GeoNames
from geoutils.dbManager import ESWrapper
from collections import defaultdict
from urllib.parse import urlparse
from geoutils import LocationDistribution
import logging
from geoutils import encode, isempty
import json
import pdb
import re
from collections import Counter

numstrip=re.compile("\d")
tracer = logging.getLogger('elasticsearch')
tracer.setLevel(logging.CRITICAL)  # or desired level
tracer = logging.getLogger('urllib3')
tracer.setLevel(logging.CRITICAL)  # or desired level
# tracer.addHandler(logging.FileHandler('indexer.log'))
logging.basicConfig(filename='geocode.log', level=logging.DEBUG)
log = logging.getLogger("rssgeocoder")


class BaseGeo(object):
    def __init__(self, db, min_popln=0, min_length=1, spacy=False, nerKeyMap=None):

        DEFAULT_NER_MAP = {'LOCATION': 'LOCATION', 'ORGANIZATION': 'ORGANIZATION',
                'NATIONALITY': 'NATIONALITY', 'OTHER': 'OTHER', 'PERSON': 'PERSON'}

        if nerKeyMap is None:
            nerKeyMap = DEFAULT_NER_MAP
        else:
            for key in DEFAULT_NER_MAP:
                if key not in nerKeyMap:
                    nerKeyMap[key] = DEFAULT_NER_MAP[key]

        if spacy is True:
            nerKeyMap['LOCATION'] = 'GPE'
            nerKeyMap['NATIONALITY'] = 'NORP'
            nerKeyMap['ORGANIZATION'] = 'ORG'

        self.nerKeyMap = nerKeyMap
        self.gazetteer = GeoNames(db)
        self.min_popln = min_popln
        self.min_length = min_length
        self.weightage = {
            nerKeyMap["LOCATION"]: 1.0,
            nerKeyMap["NATIONALITY"]: 0.75,
            nerKeyMap["ORGANIZATION"]: 0.5,
            nerKeyMap["OTHER"]: 0.0
        }

    def geocode(self, doc=None, loclist=None, eKey='BasisEnrichment', **kwargs):
        locTexts = []
        NAMED_ENTITY_TYPES_TO_CHECK = [key for key in self.weightage if self.weightage[key] > 0]
        if doc is not None:
            # Get all location entities from document with atleast min_length characters
            locTexts += [(numstrip.sub("", l['expr'].lower()).strip(), l['neType']) for l in
                         doc[eKey]["entities"]
                         if ((l["neType"] in NAMED_ENTITY_TYPES_TO_CHECK) and
                             len(l['expr']) >= self.min_length)]

            # locTexts += [(numstrip.sub("", l['expr'].lower()).strip(), 'OTHER') for l in
                         # doc['BasisEnrichment']['nounPhrases']]
            persons = [(numstrip.sub("", l['expr'].lower()).strip(),
                        l['neType'])
                        for l in
                        doc[eKey]["entities"]
                        if ((l["neType"] == self.nerKeyMap["PERSON"]) and
                        len(l['expr']) >= self.min_length)]

        if loclist is not None:
            locTexts += [l.lower() for l in loclist]

        results = self.get_locations_fromURL((doc["url"] if doc.get("url", "")
                                              else doc.get("link", "")))
        # results = {}
        # kwargs['analyzer'] = 'standard'
        return self.geocode_fromList(locTexts, persons, results, **kwargs)

    def geocode_fromList(self, locTexts, persons, results=None, min_popln=None, **kwargs):
        if results is None:
            results = {}

        if min_popln is None:
            min_popln = self.min_popln

        itype = {}
        realized_countries = []
        for l in locTexts:
            if l == "":
                continue
            if isinstance(l, tuple):
                itype[l[0]] = l[1]
                l = l[0]
            else:
                itype[l] = self.nerKeyMap['LOCATION']
            try:
                if l in results:
                    results[l].frequency += 1
                else:
                    for sub in l.split(","):
                        sub = sub.strip()
                        if sub in results:
                            results[sub].frequency += 1
                        else:
                            itype[sub] = itype[l]
                            try:
                                results[sub] = self._queryitem(sub, itype[sub])
                                if len(results[sub].realizations) == 1:
                                    realized_countries.append(list(results[sub].realizations.values())[0]['countryCode'].lower())
                            except UnicodeDecodeError:
                                pdb.set_trace()
                            results[sub].frequency = 1
            except UnicodeDecodeError:
                log.exception("Unable to make query for string - {}".format(encode(l)))

#         realized_countries = Counter(realized_countries)
#         co_realized = float(sum(realized_countries.values()))
#         selco = [kl for kl, vl in realized_countries.viewitems()
#                                                  if float(vl/co_realized) >= 0.5]
#         try:
#             selco = realized_countries.most_common(n=1)[0][0]
#         except:
#             selco = []
        selco = list(set(realized_countries))

        if kwargs.get('doFuzzy', False) is True and selco not in (None, "", []):
            results = self.fuzzyquery(results,
                                      countryFilter=selco)

        persons_res = {}
        if kwargs.get('includePersons', False) is True:
            for entitem in persons:
                querytext, _ = entitem
                if querytext not in persons_res:
                    persons_res[querytext] = {"expansions": self._queryitem(querytext, "LOCATION", countryCode=selco),
                                              "freq": 1}
                    if querytext not in results:
                        results[querytext] = persons_res[querytext]['expansions']
                        results[querytext].frequency = 1
                else:
                    persons_res[querytext]["freq"] += 1
                    results[querytext].frequency += 1

        scores = self.score(results)
        custom_max = lambda x: max(x.values(),
                                   key=lambda y: y['score'])
        lrank = self.get_locRanks(scores, results)
        lmap = {l: custom_max(lrank[l]) for l in lrank if not lrank[l] == {}}
        total_weight = sum([self.weightage[itype.get(key, 'OTHER')] for key in lmap]) + 1e-3
        return lmap, max(lmap.items(),
                         key=lambda x: x[1]['score'] * self.weightage[itype.get(x[0], 'OTHER')] / total_weight)[1]['geo_point'] if scores else {}


    def _queryitem(self, item, itemtype, **kwargs):
        if itemtype == self.nerKeyMap["LOCATION"]:
            res = self.gazetteer.query(item, **kwargs)
        else:
            res = self.gazetteer.query(item, fuzzy='AUTO' if kwargs.get('doFuzzy', False) else 0, featureCode='pcli', operator='or')
            if res == []:
                res = self.gazetteer.query(item, featureCode='adm1', operator='or')

        return LocationDistribution(res)

    def get_locations_fromURL(self, url):
        """
        Parse URL to get URL COUNTRY and also URL SUBJECT like taiwan in
        'cnn.com/taiwan/protest.html'

        Params:
            url - a web url

        Returns:
            Dict of locations obtained from URL
        """
        results = {}
        urlinfo = urlparse(url)
        if urlinfo.netloc != "":
            urlsubject = urlinfo.path.split("/", 2)[1]
            urlcountry = urlinfo.netloc.rsplit(".", 1)[-1]
            # Find URL DOMAIN Country from 2 letter iso-code
            if len(urlcountry.strip()) == 2:
                urlcountry = self.gazetteer.get_country(urlcountry.upper())
                if urlcountry != []:
                    urlcountry = urlcountry[0]
                    urlcountry.confidence = 1.0
                    results["URL-DOMAIN_{}".format(urlcountry)] = LocationDistribution(urlcountry)
                    results["URL-DOMAIN_{}".format(urlcountry)].frequency = 1

            if 5 < len(urlsubject) < 20:
                usubj_q = self.gazetteer.query(urlsubject, 15000)
                if usubj_q:
                    results["URL-SUBJECT_{}".format(urlsubject)] = LocationDistribution(usubj_q)
                    results["URL-SUBJECT_{}".format(urlsubject)].frequency = 1
        return results

    def fuzzyquery(self, locmap, countryFilter=[]):
        for loc in locmap:
            if len(locmap[loc].realizations) != 1:
                freq = locmap[loc].frequency
                subres = self.gazetteer.query(loc, countryCode=countryFilter, fuzzy='AUTO')
                if subres != []:
                    pts = subres +locmap[loc].realizations.values()
                    ldist = self.gazetteer._get_loc_confidence(pts, self.min_popln)
                    locmap[loc] = LocationDistribution(ldist)
                    locmap[loc].frequency = freq
        return locmap

    def annotate(self, doc, enrichmentKeys=['BasisEnrichment'], **kwargs):
        """
        Attach embersGeoCode to document
        """
        eKey = None
        for key in enrichmentKeys:
            if key in doc and doc[key]:
                eKey = key

        if eKey is None:
            return doc

        try:
            lmap, gp = self.geocode(doc=doc, eKey=eKey, **kwargs)
        except UnicodeDecodeError as e:
            log.exception("unable to geocode:{}".format(str(e)))
            lmap, gp = {}, {}

        doc['embersGeoCode'] = gp
        doc["location_distribution"] = lmap
        return doc

    def score(self, results):
        scoresheet = defaultdict(float)
        num_mentions = float(sum((l.frequency for l in results.values())))

        def update(l):
            for s in l.city:
                scoresheet[s] += l.city[s] * l.frequency
            for s in l.admin1:
                scoresheet[s] += l.admin1[s] * l.frequency
            for s in l.country:
                scoresheet[s] += l.country[s] * l.frequency

        _ = [update(item) for item in results.values()]
        for s in scoresheet:
            scoresheet[s] /= num_mentions

        return scoresheet

    def get_locRanks(self, scores, loc_cand):
        """
        Each city score needs to be re-inforced with the
        corresponding state and country scores to get the actual meaning
        of that name. For example, several mentions of cities within virginia
        would have given virginia
        state a high score. Now this high score has to be brought back to lower levels to
        decide on meaning of each name/city
        """
        loc_rankmap = {}

        def get_realization_score(l):
            lscore_map = {}
            for lstr, r in l.realizations.items():
                base_score = scores[lstr]
                #if r.ltype == 'city':
                if not isempty(r.city):
                    l_adminstr = '/'.join([r.country, r.admin1, ''])
                    base_score = (base_score + scores[l_adminstr] + scores[r.country + "//"]) * r.confidence

                elif not isempty(r.admin1):
                    base_score = (base_score + scores[r.country + "//"]) * r.confidence

                elif r.ltype == "country":
                    # do nothing
                    pass
                else:
                    base_score = base_score * r.confidence
                    # code for other types
                    #if not isempty(r.city):
                    #    l_adminstr = '/'.join([r.country, r.admin1, ''])
                    #    base_score = (base_score + scores[l_adminstr] + scores[r.country + "//"]) * r.confidence

                    #ipdb.set_trace()
                    #raise Exception("Unknown location type-{} for {}".format(r.ltype, lstr))

                lscore_map[lstr] = {'score': base_score, 'geo_point': r.__dict__}

            #for s in l.realizations:
            #    base_score = scores[s]
            #    if l.realizations[s].ltype not in ('country', 'admin'):
            #        l_adminstr = encode('/'.join([l.realizations[s].country,
            #                               l.realizations[s].admin1, '']))

            #        base_score += scores[l_adminstr] + scores[l.realizations[s].country]

            #    elif l.realizations[s].ltype == 'admin':
            #        base_score += scores[l.realizations[s].country]

            #    lscore_map[s] = {'score': base_score, 'geo_point': l.realizations[s].__dict__}
            return lscore_map

        for locpt in loc_cand:
            loc_rankmap[locpt] = get_realization_score(loc_cand[locpt])
        return loc_rankmap


class TextGeo(object):
    def __init__(self, dbpath="./Geonames_dump.sql", min_popln=0, coverageLength=10):
        """
        Description
        """
        self.coverageLength = coverageLength
        self.gazetteer = GeoNames("./Geonames_dump.sql")
        self.min_popln = min_popln

    def geocode(self, doc):
        """

        """
        def getEntityDetails(entity):
            """
            return entity string, starting offset, coverage end point
            """
            start, end = entity['offset'].split(":")
            start, end = int(start), int(end)
            return (entity['expr'], start,
                    start - self.coverageLength,
                    end + self.coverageLength)

        urlinfo = urlparse(doc["url"])
        loc_results = {}
        locTexts = [getEntityDetails(l) for l in doc["BasisEnrichment"]['entities']
                    if l['neType'] == 'LOCATION']
        if urlinfo.netloc != "":
            urlsubject = urlinfo.path.split("/", 2)[1]
            urlcountry = urlinfo.netloc.rsplit(".", 1)[-1]
            if len(urlcountry.strip()) == 2:
                urlcountry = self.gazetteer.get_country(urlcountry.upper())
                if urlcountry != []:
                    urlcountry = urlcountry[0]
                    urlcountry.confidence = 1.0
                    loc_results["url"] = LocationDistribution(urlcountry)
                    loc_results["url"].frequency = 1
            if len(urlsubject) < 20:
                locTexts.insert(0, (urlsubject, -1, -1, -1))

        loc_results.update(self.query_gazetteer(self.group(locTexts)))

        scores = self.score(loc_results)
        custom_max = lambda x: max(x.realizations.values(),
                                   key=lambda x: scores[x.__str__()])
        lmap = {l: custom_max(loc_results[l]['geo-point']) for l in loc_results
                if not loc_results[l]['geo-point'].isEmpty()}
        egeo = {}
        if scores:
            egeo = scores[max(scores, key=lambda x: scores[x])]
        return lmap, egeo

    def score(self, results):
        scoresheet = defaultdict(float)

        def update(item):
            l = item['geo-point']
            freq = item['frequency']
            for s in l.city:
                scoresheet[s] += l.city[s] * freq
            for s in l.admin1:
                scoresheet[s] += l.admin1[s] * freq
            for s in l.country:
                scoresheet[s] += l.country[s] * freq

        [update(item) for item in results.values()]
        return scoresheet

    def query_gazetteer(self, lgroups):
        """
        get Location groups
        """
        gp_map = {}
        query_gp = lambda x: self.gazetteer.query(x) if x not in gp_map else gp_map[x]
        for grp in lgroups:
            imap = {txt: query_gp(txt) for txt in grp}
            imap = self.get_geoPoints_intersection(imap)
            for l in imap:
                if l in gp_map:
                    gp_map[l]['frequency'] += 1
                else:
                    gp_map[l] = {'geo-point': imap[l], 'frequency': 1}

            #gp_map.update(imap)

        for l in gp_map:
            gp_map[l]['geo-point'] = LocationDistribution(gp_map[l]['geo-point'])

        return gp_map

    def group(self, loc):
        groups = []
        i = 0
        while i < len(loc):
            grp = [loc[i][0]]
            for j, l in enumerate(loc[i + 1:]):
                if l[1] <= loc[i][-1]:
                    grp.append(l[0])
                    i += 1
                else:
                    groups.append(grp)
                    i += 1
                    grp = [loc[i][0]]
                    break
            else:
                groups.append(grp)
                i += 1
        return groups

    def get_geoPoints_intersection(self, gps):
        try:
            selcountry = set.intersection(*[set([l.country])
                                            for name in gps for l in gps[name]])
        except:
            selcountry = None

        if not selcountry:
            return gps

        selcountry = selcountry.pop()
        filtered_gps = [set([encode('/'.join([l.country, l.admin1, ""]))]) for name in gps
                        for l in gps[name] if l.country == selcountry]

        sel_admin1 = set.intersection(*filtered_gps)
        if not sel_admin1:
            return {name: [l for l in gps[name] if l.country == selcountry]
                    for name in gps}

        sel_admin1 = sel_admin1.pop()
        ns = {}
        for l in gps:
            t_admin = [gp for gp in gps[l] if gp.__str__() == sel_admin1]
            if t_admin != []:
                ns[l] = t_admin
                continue
            t_cand = [gp for gp in gps[l]
                      if encode("/".join([gp.country, gp.admin1, ""])) == sel_admin1]
            ns[l] = t_cand
        return ns


def tmpfun(doc):
    try:
        msg = json.loads(doc)
        msg = GEO.annotate(msg, enrichmentKeys=['BasisEnrichment', ''])
        return msg
    except Exception as e:
        print("error-{}".format(str(e)))


if __name__ == "__main__":
    import sys
    import argparse
    from geoutils import smart_open
    parser = argparse.ArgumentParser()
    parser.add_argument("--cat", "-c", action='store_true',
                        default=False, help="read from stdin")
    parser.add_argument("-i", "--infile", type=str, help="input file")
    parser.add_argument("-o", "--outfile", type=str, help="output file")
    args = parser.parse_args()

    db = ESWrapper(index_name="geonames2", doc_type="places2")
    GEO = BaseGeo(db)

    if args.cat:
        infile = sys.stdin
        outfile = sys.stdout
    else:
        infile = smart_open(args.infile)
        outfile = smart_open(args.outfile, "wb")

    lno = 0
    wp = WorkerPool(infile, outfile, tmpfun, 500)
    wp.run()
    # for l in infile:
        # try:
            # j = json.loads(l)
            # j = GEO.annotate(j)
            # #log.debug("geocoded line no:{}, {}".format(lno,
            # #                                           encode(j.get("link", ""))))
            # lno += 1
            # outfile.write(encode(json.dumps(j, ensure_ascii=False) + "\n"))
        # except UnicodeEncodeError:
            # ipdb.set_trace()
            # log.exception("Unable to readline")
    #         continue

    if not args.cat:
        infile.close()
        outfile.close()

    exit(0)
