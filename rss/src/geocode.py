#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"



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
