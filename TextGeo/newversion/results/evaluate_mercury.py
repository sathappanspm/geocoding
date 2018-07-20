#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"
import sys
sys.path.append('../')
# sys.path.append('../')
from geocode import BaseGeo
from geoutils.dbManager import ESWrapper
from geoutils import geodistance
import json
import ipdb


def _equals(loc1, loc2):
    co, st, ci = 0, 0, 0
    if loc1 == {} or loc2 == {}:
        return co, st, ci
    if loc1['country'].lower() == loc2['country'].lower():
        co = 1
        if loc2['admin1'].lower() == loc1['admin1'].lower():
            st = 1
            if(loc1['city'].lower() == loc2['city'].lower()):
                if (loc1.get('name', loc1['city']).lower() != loc2.get('name', loc2['city']).lower()):
                    # ipdb.set_trace()
                    pass
                ci = 1

    return co, st, ci

def equals(loc1, locations):
    #co, st, ci = _equals(loc1, locations)
    co, st, ci = 0, 0, 0 #_equals(loc1, locations)
    for l in locations:
        if 'geo_point' in l:
            l = l['geo_point']

        c1, st1, ci1 = _equals(loc1, l)
        co = (co or c1)
        st = (st or st1)
        ci = (ci or ci1)
    return co, st, ci

def get_groundTruthInfo(loc, geo):
    info = geo.gazetteer.get_locInfo(country=loc['Country'], admin=loc['State'], city=loc['City'])
    #if len(info) > 1:
    #   # ipdb.set_trace()
    #    info2 = [l for l in info if l['ltype'] == 'city']
    #    if len(info2) != 0:
    #        info = info2
    #        #ipdb.set_trace()
    if info == []:
        ipdb.set_trace()
    return info[0]


def main(args):
    normal_perf = {'country': 0, 'state': 0, 'city': 0}
    geonames_perf = {'country': 0, 'state': 0, 'city': 0}
    distance_perf = {'country': 0, 'state': 0, 'city': 0}
    db = ESWrapper('geonames', 'places')
    geo = BaseGeo(db, 'Uniform')
    linec = 0
    with open(args.infile) as inf:
        num_evts = 0
        empty = 0
        for ln in inf:
            msg = json.loads(ln)
            if msg['embersGeoCode'] == {}:
                empty += 1
                continue

            linec += 1
            if linec > 1000:
                break
            num_evts += 1
            co, st, ci = 0, 0, 0
            trueloc = {"country": msg['events'][0]['Country'], 'admin1': msg['events'][0]['State'] or '',
                       'city': msg['events'][0]['City'] or ''}
            # co, st, ci = equals(trueloc, msg['location_distribution'].values())
            if args.all:
                co, st, ci = equals(trueloc, msg['location_distribution'].values())
            else:
                co, st, ci = equals(trueloc, [msg['embersGeoCode']])

            # co, st, ci = equals(trueloc, msg['embersGeoCode'])
            normal_perf['country'] += co
            normal_perf['state'] += st
            normal_perf['city'] += ci

            co, st, ci = 0, 0, 0
            try:
                msg['groundTruth'] = get_groundTruthInfo(msg['events'][0], geo)
            except Exception as e:
                # ipdb.set_trace()
                print(str(e))
                print(trueloc), msg['events'][0]['Latitude'], msg['events'][0]['Longitude']
                msg['groundTruth'] = trueloc

            if args.all:
                co, st, ci = equals(trueloc, msg['location_distribution'].values())
            else:
                co, st, ci = equals(trueloc, [msg['embersGeoCode']])

            geonames_perf['country'] += co
            geonames_perf['state'] += st
            geonames_perf['city'] += ci

            co, st, ci = 0, 0, 0
            if msg['events'][0]['Country'].lower() == msg['embersGeoCode'].get('country', '').lower():
                co = 1.0
                if msg['events'][0]['State'] in (None, '-', '') and msg['events'][0]['City'] in (None, '-', ''):
                    st, ci = 1.0, 1.0
                else:
                    try:
                        true_lon, true_lat = msg['events'][0]['Longitude'], msg['events'][0]['Latitude']
                        pred_lon, pred_lat = msg['embersGeoCode']['coordinates']
                        dist = geodistance([(true_lon, true_lat)], [(pred_lon, pred_lat)])[0][0]
                        if dist <= 10:
                            st, ci = 1.0, 1.0
                            # if not equals(msg['groundTruth'], msg['embersGeoCode'])[1]:
                            #   ipdb.set_trace()
                        else:
                            score = 1 - min(dist, 300.0) / 300
                            st, ci = score, score
                    except Exception, e:
                        st, ci = 0.0, 0.0
                        pass
            distance_perf['country'] += co
            distance_perf['state'] += st
            distance_perf['city'] += ci

    import pandas as pd

    df = pd.DataFrame([normal_perf, geonames_perf, distance_perf],
                      index=['normal', 'geonames', 'distance'])
    print(df / num_evts)
    print("No geocode for ", empty)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile", default='sina_geocoded_protest.json')
    ap.add_argument("--all", action='store_true', default=False)
    args = ap.parse_args()
    main(args)
