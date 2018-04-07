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

import json
import gzip
from geoutils.dbManager import ESWrapper
from geocode_twitter import TweetGeocoder
from embers.geocode import Geo, decode
from embers.geocode_mena import GeoMena as MENAGEO

DB = ESWrapper('geonames', 'places')
GEO = TweetGeocoder(DB)
ptrue, pfalse = 0, 0

error = open("error_colombia.txt", "w")
mGeo = MENAGEO()
eGeo = Geo()

eGeo = mGeo

def embersgeo(doc):
    msg = json.loads(doc)
    try:
        lt, ln, places, texts, enr = eGeo._normalize_payload(msg)
        true_geo = eGeo._geo_normalize(lt, ln, None, {}, None, eGeo.priority_policy)
        true_geo = {"city": decode(true_geo[0]),
                "country": decode(true_geo[1]),
                "admin1": decode(true_geo[2])
                }
        if not true_geo['country']:
            #print "error"
            true_geo = {}

        #texts.pop("text")
        #texts.pop("user_description")
        #texts.pop("language",'')
        #txt = {"user_location": texts.pop("user_location",'')}
        eCode = eGeo._geo_normalize(None, None, places, texts, enr, eGeo.priority_policy)
        eCode = {"city": decode(eCode[0]),
                "country": decode(eCode[1]),
                "admin1": decode(eCode[2])
                }
        if not eCode['country']:
            eCode = {}
        #msg['true_geo'] = true_geo
        msg['embersGeoCode'] = eCode
        msg['eGeo'] = eCode
        #print eCode
        return msg
    except Exception as e:
        print str(e)
        error.write(doc)

    return None

def geotest(doc):
    try:
        msg = json.loads(doc)
    except:
        return None

    try:
        msg.pop('embersGeoCode',"")
        msg.pop('esGeo', "")
        msg = GEO.evaluate(msg)
        #msg['esGeo'] = msg['embersGeoCode']
        return msg
    except:
        error.write(doc)

    return None

with gzip.open("Egypt_evaluated_embers4_tp5.txt.gz", "w") as outf:
    #with gzip.open("Colombia_evaluated_embers2.txt.gz") as inf:
    #with open("./Egypt_embers.txt") as inf:
    #with gzip.open("Egypt_evaluated_embers4_tt.txt.gz") as inf:
    with open("./twrong2.txt") as inf:
    #with gzip.open("/home/sathappan/shared/datasets/tweets_geocoded_byCountry/Colombia.txt.gz") as inf:
    #with gzip.open("/home/sathappan/shared/datasets/tweets_geocoded_byCountry/United_States.txt.gz") as inf:
        wp = WorkerPool(inf, outf, geotest, 10)
        #wp = WorkerPool(inf, outf, embersgeo, 1)
        wp.run()
        print wp._true, wp._false, wp._nogeo, wp._notruth
        #for l in inf:
        #    msg = geotest(l)
        #    outf.write(json.dumps(msg, ensure_ascii=False).encode("utf-8") + "\n")
