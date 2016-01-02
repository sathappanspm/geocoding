#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from geoutils.gazetteer import GeoNames
import ipdb
from collections import defaultdict
from urlparse import urlparse
from geoutils import LocationDistribution


class BaseGeo(object):
    def __init__(self, min_popln=0):
        self.gazetteer = GeoNames("./Geonames_dump.sql", "")
        self.min_popln = min_popln

    def geocode(self, doc):
        locTexts = [(l['expr'], self.min_popln) for l in doc["BasisEnrichment"]["entities"]
                    if l["neType"] == "LOCATION"]
        results = {}
        urlinfo = urlparse(doc["url"])
        if urlinfo.netloc != "":
            urlsubject = urlinfo.path.split("/", 2)[1]
            urlcountry = urlinfo.netloc.rsplit(".", 1)[-1]
            if len(urlcountry.strip()) == 2:
                urlcountry = self.gazetteer.get_country(urlcountry.upper())
                if urlcountry != []:
                    urlcountry = urlcountry[0]
                    urlcountry.confidence = 1.0
                    results["url"] = LocationDistribution(urlcountry)
            if len(urlsubject) < 20:
                locTexts.append((urlsubject, 15000))

        for l in locTexts:
            results[l[0]] = LocationDistribution(self.gazetteer.query(l[0], min_popln=l[1]))

        scores = self.score(results)
        custom_max = lambda x: max(x.viewvalues(),
                                   key=lambda y: y['score'])
        lrank = self.get_locRanks(scores, results)
        lmap = {l: custom_max(lrank[l]) for l in lrank if not lrank[l] == {}}
        #if len(lmap) > 5:
            #ipdb.set_trace()
        #return lmap, max(scores, key=lambda x: scores[x]) if scores else "//"
        return lmap, scores, max(lmap.values(), key=lambda x: x['score'])['geo_point'] if scores else "//"

    def score(self, results):
        scoresheet = defaultdict(float)

        def update(l):
            for s in l.city:
                scoresheet[s] += l.city[s]
            for s in l.admin1:
                scoresheet[s] += l.admin1[s]
            for s in l.country:
                scoresheet[s] += l.country[s]
        [update(item) for item in results.viewvalues()]
        return scoresheet

    def get_locRanks(self, scores, loc_cand):
        """
        Each city score needs to be re-inforced with the
        corresponding state and country scores to get the actual meaning
        of that name. For example, several mentions of cities within virginia would have given virginia
        state a high score. Now this high score has to be brought back to lower levels to
        decide on meaning of each name/city
        """
        loc_rankmap = {}

        def get_realization_score(l):
            lscore_map = {}
            for s in l.realizations:
                base_score = scores[s]
                if l.realizations[s].ltype not in ('country', 'admin'):
                    l_adminstr = '/'.join([l.realizations[s].country,
                                           l.realizations[s].admin1, ''])

                    base_score += scores[l_adminstr] + scores[l.realizations[s].country]

                elif l.realizations[s].ltype == 'admin':
                    base_score += scores[l.realizations[s].country]

                lscore_map[s] = {'score': base_score, 'geo_point': l.realizations[s].__dict__}
            return lscore_map

        for locpt in loc_cand:
            loc_rankmap[locpt] = get_realization_score(loc_cand[locpt])
        return loc_rankmap


class TextGeo(object):
    def __init__(self, min_popln=0, coverageLength=10):
        """
        Description
        """
        self.coverageLength = coverageLength
        self.gazetteer = GeoNames("./Geonames_dump.sql", "")
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
            if len(urlsubject) < 20:
                locTexts.insert(0, (urlsubject, -1, -1, -1))

        loc_results.update(self.query_gazetteer(self.group(locTexts)))

        scores = self.score(loc_results)
        custom_max = lambda x: max(x.realizations.viewvalues(),
                                   key=lambda x: scores[x.__str__()])
        lmap = {l: custom_max(loc_results[l]) for l in loc_results
                if not loc_results[l].isEmpty()}
        if len(lmap) > 5:
            ipdb.set_trace()
        return lmap, max(scores, key=lambda x: scores[x]) if scores else "//"

    def score(self, results):
        scoresheet = defaultdict(float)

        def update(l):
            for s in l.city:
                scoresheet[s] += l.city[s]
            for s in l.admin1:
                scoresheet[s] += l.admin1[s]
            for s in l.country:
                scoresheet[s] += l.country[s]

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
            gp_map.update(imap)

        for l in gp_map:
            gp_map[l] = LocationDistribution(gp_map[l])

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
        filtered_gps = [set(['/'.join([l.country, l.admin1, ""])]) for name in gps
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
                      if "/".join([gp.country, gp.admin1, ""]) == sel_admin1]
            ns[l] = t_cand
        return ns
