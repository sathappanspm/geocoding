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
        self.gazetteer = GeoNames("../../../Geonames_dump.sql", "")
        self.min_popln = min_popln

    def geocode(self, doc):
        locTexts = [l['expr'] for l in doc["BasisEnrichment"]["entities"]
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
                locTexts.append(urlsubject)

        for l in locTexts:
            results[l] = self.gazetteer.query(l, min_popln=self.min_popln)

        scores = self.score(results)
        #if scores:
        lmap = {}
        for l in results:
            if results[l]:
                lmap[l] = max(results[l].baseObj.viewvalues(), key=lambda x: scores[x.__str__()])

        return lmap, max(scores, key=lambda x: scores[x]) if scores else "//"
        #return "//"

    def score(self, results):
        scoresheet = defaultdict(float)

        def update(l):
            for s in l.city:
                scoresheet[s] += l.city[s]
            for s in l.admin1:
                scoresheet[s] += l.admin1[s]
            for s in l.country:
                scoresheet[s] += l.country[s]
        _ = [update(item) for item in results.viewvalues()]
        return scoresheet


class TextGeo(object):
    def __init__(self, coverageLength=2):
        """
        Description
        """
        pass

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
                    start - coverageLength,
                    end + coverageLength)

        locTexts = [getEntityDetails(l) for l in l["BasisEnrichment"]['entities']
                   if l['neType'] == 'LOCATION']
        locGroups = self.group(locTexts)
        self.geo_resolver.resolveAll(locGroups)

    def group(self, loc):
        groups = []
        i = 0
        while i < len(loc) - 1:
            grp = (loc[i],)
            for j, l in enumerate(loc[i:]):
                if l[1] <= loc[i][-1]:
                    grp += (l[1],)
                    i = j
                else:
                    groups.append(grp)
                    i = j
                    break
            else:
                i += 1

        return groups




if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument()
    args = ap.parse_args()
    main(args)
