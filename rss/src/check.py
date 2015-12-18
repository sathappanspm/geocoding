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
from geocode import BaseGeo
from geoutils import encode

def decode(s):
    try:
        return s.decode("utf-8")
    except:
        return s


CODES={"Argentina": "AR", "Brazil": "BR", "Colombia": "CO",  "Chile": "CL", "Ecuador": "EC",
       "El Salvador": "SV", "Mexico": "MX", "Venezuela": "VE", "Paraguay": "PY",
       "Uruguay": "UY"}

#s = SQLiteWrapper("./WG_dump.sql")
# with open("../data/gsr_enriched_201412.txt") as infile:
#with open("../data/gsr_basisEnriched_201412.txt.gz") as infile:
with open("./gsrfiltered.txt") as infile:
    cnt = 0
    tcnt = 0
    bg = BaseGeo(min_popln = 1)
    err = 0
    ccnt = 0
    for l in infile:
        j = json.loads(l)
        tcnt += 1
        try:
            lmap, loc = bg.geocode(j)
        except Exception, e:
            err += 1
            continue
        country = loc.split("/")[0]
        if country == CODES[j["location"][0]]:
            cnt += 1
        else:
            for l in lmap:
                if j["location"][-1].encode("utf-8") in lmap[l].__str__():
                    ccnt += 1

        if tcnt % 100 == 0:
            print cnt, ccnt, err, tcnt
            #if tcnt == 1000:
            break

print cnt, ccnt, tcnt
