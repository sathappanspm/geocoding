#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"
import json
from geoutils.gazetteer import GeoNames
from unidecode import unidecode
import ipdb

def main(args):
    gn = GeoNames(dbpath="./geoutils/GN_dump_20160407.sql")
    with open(args.infile) as infile:
        with open(args.outfile, "w") as outfile:
            for l in infile:
                j = json.loads(l)
                try:
                    country, state, city = j["location"]
                except:
                    ipdb.set_trace()
                try:
                    gdict = gn.get_locInfo(country=country, admin=state, city=city, strict=False)[0].__dict__
                except Exception,e:
                    #print str(e)
                    try:
                        gdict = gn.get_locInfo(country=country, admin=state, city=unidecode(city).split('-')[0], strict=False)[0].__dict__
                    except Exception, e:
                        #print "loc2, ", str(e)
                        #ipdb.set_trace()
                        print j['location']
                        continue
                j['latitude'], j['longitude'] = gdict['latitude'], gdict['longitude']
                outfile.write(json.dumps(j, ensure_ascii=False).encode("utf-8") + "\n")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--infile" , type=str)
    ap.add_argument("--outfile" , type=str)
    args = ap.parse_args()
    main(args)
