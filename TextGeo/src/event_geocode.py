#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

from workerpool import WorkerPool
__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"
from geocode import BaseGeo
from geoutils.dbManager import ESWrapper
import json

db = ESWrapper('geonames', 'places')
GEO = BaseGeo(db=db)

def disambiguate_event_loc(data):
    for evt in data['events']:
        evt['expanded_loc'] = []
        for eloc in evt['City'].split(";"):
            try:
                loc = GEO.gazetteer.get_locInfo(country=evt['Country'],
                                                admin=evt['State'],
                                                city=evt["City"])
                evt['expanded_loc'].append(loc)
            except Exception as e:
                try:
                    print("exception for event-{} , {}".format(str(e), evt['City'].encode('utf-8')))
                except:
                    pass

    return data

def annotate(ln):
    data = json.loads(ln)
    data = disambiguate_event_loc(data)
    data = GEO.annotate(data)
    return data


if __name__ == "__main__":
    import gzip
    with gzip.open("DME_eventsGeoExpanded_Oct24.mjson.gz", "w") as outf:
        with gzip.open("/home/sathap1/workspace/eventClf/data/DME_langTimeGeoEnriched_events.mjson.gz") as inf:
            wp = WorkerPool(inf, outf, annotate, 300, maxcnt=None)
            wp.run()
