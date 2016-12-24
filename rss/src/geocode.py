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
from geoutils.gazetteer import GeoNames
from collections import defaultdict
from urlparse import urlparse
from geoutils import LocationDistribution
import logging
from geoutils import encode


logging.basicConfig(filename='geocode.log', level=logging.DEBUG)
log = logging.getLogger("rssgeocoder")


class BaseGeo(object):
    def __init__(self, dbpath="./Geonames_dump.sql", min_popln=0, min_length=1):
        self.gazetteer = GeoNames(dbpath)
        self.min_popln = min_popln
        self.min_length = min_length

    def geocode(self, doc=None, loclist=None):
        locTexts = []
        if doc is not None:
            # Get all location entities from document with atleast min_length characters
            locTexts += [l['expr'].lower() for l in doc["BasisEnrichment"]["entities"]
                         if ((l["neType"] == "LOCATION") and
                             len(l['expr']) >= self.min_length)]

        if loclist is not None:
            locTexts += [l.lower() for l in loclist]

        results = self.get_locations_fromURL((doc["url"] if doc.get("url", "")
                                              else doc.get("link", "")))
        return self.geocode_fromList(locTexts, results)

    def geocode_fromList(self, locTexts, results=None, min_popln=None):
        if results is None:
            results = {}

        if min_popln is None:
            min_popln = self.min_popln

        for l in locTexts:
            try:
                if l in results:
                    results[l].frequency += 1
                else:
                    q = self.gazetteer.query(l, min_popln=min_popln)
                    if not q:
                        for sub in l.split(","):
                            sub = sub.strip()
                            if sub in results:
                                results[sub].frequency += 1
                            else:
                                results[sub] = LocationDistribution(self.gazetteer.query(sub, min_popln=min_popln))
                                results[sub].frequency = 1
                    else:
                        results[l] = LocationDistribution(q)
                        results[l].frequency = 1
            except:
                log.exception("Unable to make query for string - {}".format(encode(l)))

        scores = self.score(results)
        custom_max = lambda x: max(x.viewvalues(),
                                   key=lambda y: y['score'])
        lrank = self.get_locRanks(scores, results)
        lmap = {l: custom_max(lrank[l]) for l in lrank if not lrank[l] == {}}
        #ipdb.set_trace()
        return lmap, max(lmap.values(), key=lambda x: x['score'])['geo_point'] if scores else {}

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

            if self.min_length < len(urlsubject) < 20:
                usubj_q = self.gazetteer.query(urlsubject, 15000)
                if usubj_q:
                    results["URL-SUBJECT_{}".format(urlsubject)] = LocationDistribution(usubj_q)
                    results["URL-SUBJECT_{}".format(urlsubject)].frequency = 1
        return results

    def annotate(self, doc):
        """
        Attach embersGeoCode to document
        """
        try:
            lmap, gp = self.geocode(doc=doc)
        except Exception, e:
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

        _ = [update(item) for item in results.viewvalues()]
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
            for lstr, r in l.realizations.viewitems():
                base_score = scores[lstr]
                if r.ltype == 'city':
                    l_adminstr = '/'.join([r.country, r.admin1, ''])
                    base_score = (base_score + scores[l_adminstr] + scores[r.country + "//"]) * r.confidence

                elif r.ltype == 'admin1':
                    base_score = (base_score + scores[r.country + "//"]) * r.confidence

                elif r.ltype == "country":
                    # do nothing
                    pass
                else:
                    ipdb.set_trace()
                    raise Exception("Unknown location type-{} for {}".format(r.ltype, lstr))

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
        custom_max = lambda x: max(x.realizations.viewvalues(),
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

        [update(item) for item in results.viewvalues()]
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


if __name__ == "__main__":
    import sys
    import argparse
    import json
    from geoutils import smart_open
    parser = argparse.ArgumentParser()
    parser.add_argument("--cat", "-c", action='store_true',
                        default=False, help="read from stdin")
    parser.add_argument("-i", "--infile", type=str, help="input file")
    parser.add_argument("-o", "--outfile", type=str, help="output file")
    args = parser.parse_args()
    geo = BaseGeo(dbpath="./geoutils/GN_dump_20160407.sql", min_length=3)
    if args.cat:
        infile = sys.stdin
        outfile = sys.stdout
    else:
        infile = smart_open(args.infile)
        outfile = smart_open(args.outfile, "wb")

    lno = 0
    for l in infile:
        try:
            j = json.loads(l)
            j = geo.annotate(j)
            #log.debug("geocoded line no:{}, {}".format(lno,
            #                                           encode(j.get("link", ""))))
            #lno += 1
            outfile.write(encode(json.dumps(j, ensure_ascii=False)) + "\n")
        except:
            log.exception("Unable to readline")
            continue

    if not args.cat:
        infile.close()
        outfile.close()

    exit(0)
