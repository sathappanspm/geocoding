#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from workerpool import WorkerPool
from geoutils.dbManager import ESWrapper
from geocode_twitter import TweetGeocoder
import json
# import ipdb

#db = ESWrapper(index_name="geonames2", doc_type="places2")
               #host="http://9899c246ngrok.io",
               #port=80)



def tmpfun(doc):
    try:
        msg = json.loads(doc)
        msg = GEO.annotate(msg)
        return msg
    except UnicodeEncodeError as e:
        print(str(e))


def encode(s):
    try:
        return s.encode("utf-8")
    except:
        pass
    return s

if __name__ == "__main__":
    import sys
    import argparse
    from geoutils import smart_open
    parser = argparse.ArgumentParser()
    parser.add_argument("--cat", "-c", action='store_true',
                        default=False, help="read from stdin")
    parser.add_argument("--test", "-t", action='store_true',
                        default=False, help="read from stdin")
    parser.add_argument("-f", "--fuzzy", type=int, default=0, help="fuzzy inputs")
    parser.add_argument("-p", "--prefix", type=int, default=1, help="prefix lenght")
    parser.add_argument("-i", "--infile", type=str, help="input file")
    parser.add_argument("-o", "--outfile", type=str, help="output file")
    args = parser.parse_args()

    db = ESWrapper(index_name="geonames2", doc_type="places2")
    GEO = TweetGeocoder(db)
    #GEO = BaseGeo(db)

    if args.cat:
        infile = sys.stdin
        outfile = sys.stdout
    else:
        infile = smart_open(args.infile)
        outfile = smart_open(args.outfile, "wb")

    lno = 0
    if not args.test:
        wp = WorkerPool(infile, outfile, tmpfun, 500)
        wp.run()
    else:
        for l in infile:
            try:
                #j = json.loads(l)
                #j = GEO.annotate(j)
                j=tmpfun(l)
                #log.debug("geocoded line no:{}, {}".format(lno,
                #                                           encode(j.get("link", ""))))
                lno += 1
                outfile.write(encode(json.dumps(j, ensure_ascii=False) + "\n"))
                #if lno > 100:
                #    break
            except Exception as e:
                print(str(e))
                # ipdb.set_trace()
                # log.exception("Unable to readline")
                continue

    if not args.cat:
        infile.close()
        outfile.close()
