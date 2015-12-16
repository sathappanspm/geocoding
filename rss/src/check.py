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
from search import Search
import ipdb

def decode(s):
    try:
        return s.decode("utf-8")
    except:
        return s

#s = SQLiteWrapper("./WG_dump.sql")
s = Search("./Geonames_dump.sql", "geonames_fullText")
# with open("../data/gsr_enriched_201412.txt") as infile:
with gzip.open("../data/gsr_basisEnriched_201412.txt.gz") as infile:
    cnt = 0
    for l in infile:
        j = json.loads(l)
        cnt += 1
        # if not (j["location"][2] == "-" or j["location"][2] == ""):
        #    k = j["location"][2]
        #    try:
        #        stmt = u"select * from WorldGazetteer where name='%s'" % decode(k)
        #        res = s.query(stmt)
        #        if len(res) == 0:
        #            raise Exception("null")
        #        #q = res.next()
        #    except Exception, e:
        #        print j["location"]
        if cnt > 11:
            break
        candCities, candAdmins, candCountries = [], [], []
        pr = 0
        for k in j["BasisEnrichment"]["entities"]:
            if k["neType"] == "LOCATION":
                ci, co, ad = s.query(decode(k['expr']))
                candCities += ci
                candCountries += co
                candAdmins += ad
                pr += 1
        print j['gsrId'],
        if pr == 0:
            print "no loc data",
        res = s.score(candCities, candAdmins, candCountries)
        if res:
            print max(res, key=lambda x: res[x]), j['location']
            ipdb.set_trace()
        else:
            print candCountries, candCities, candAdmins
                #print dict(res[0])
                #try:
                #    res = s.query(u"select * from geonames where name='{0}' or alternatenames like '*{0}*'".format(decode(k['expr'])))
                #    if len(res) == 0:
                #        raise Exception("null")
                #except Exception, e:
                #    #print str(e)
                #    print unidecode(k['expr'])
