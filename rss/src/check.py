#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

#from dbManager import SQLiteWrapper
import json
from unidecode import unidecode
import gzip
import ipdb
from geocode import BaseGeo, TextGeo
from geoutils import encode

def decode(s):
    try:
        return s.decode("utf-8")
    except:
        return s


CODES={"Argentina": "AR", "Brazil": "BR", "Colombia": "CO",  "Chile": "CL", "Ecuador": "EC",
       "El Salvador": "SV", "Mexico": "MX", "Venezuela": "VE", "Paraguay": "PY",
       "Uruguay": "UY"}


def decode(s):
    try:
        return s.decode("utf-8")
    except:
        return s
#s = SQLiteWrapper("./WG_dump.sql")
# with open("../data/gsr_enriched_201412.txt") as infile:
#with open("../data/gsr_basisEnriched_201412.txt.gz") as infile:
with open("./gsrfiltered.txt") as infile:
#with open("./t.txt") as infile:
    cnt = 0
    tcnt = 0
    #bg = TextGeo(min_popln = 0)
    bg = BaseGeo(min_popln = 0)
    err = 0
    ccnt = 0
    scnt = 0
    for l in infile:
        j = json.loads(l)
        tcnt += 1
        try:
            lmap, scores, loc = bg.geocode(j)
        except KeyError, e:
            print str(e)
            err += 1
            continue
        flag = False
        try:
            loc = "/".join([loc['country'], loc['admin1'], loc['city']])
            country = loc.split("/")[0]
        except:
            country = ""
        #print loc, j["location"]
        if unidecode(decode(j["location"][-1].lower())) in unidecode(decode(j['content'])).lower():
            ccnt += 1
        if country == j["location"][0]:
            #for l in lmap:
            if unidecode(decode(j["location"][-1]).lower()) in unidecode(decode(loc)).lower():
                scnt += 1
                flag = True
            cnt += 1
        else:

            for l in lmap:
                if j["location"][-1].encode("utf-8") in lmap[l].__str__():
                    ccnt += 1
                    flag = True

        #if not flag:
            #print j["url"], loc, json.dumps(j["location"])
        #    ipdb.set_trace()
        if tcnt % 100 == 0:
            #break
            print cnt, ccnt, scnt, tcnt
            #if tcnt == 1000:

print cnt, ccnt, scnt, tcnt
