#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from geoutils.dbManager import ESWrapper
from geocode import BaseGeo
import json

db = ESWrapper(index_name="geonames", doc_type="places",
               host="http://9899c246ngrok.io",
               port=80)

GEO = BaseGeo(db)


def tmpfun(doc):
    try:
        msg = json.loads(doc)
        msg = GEO.annotate(msg, reduce=True, fuzzy=1,
                           prefix_length=1,
                           max_expansions=10)

        # reduce=True,   causes the input string to be cleaned/normalized (removal
        #                of stopwords and some more)
        #
        # fuzzy=1,       can take values (0, 1, 2). 0 means no fuzziness, 1 means
        #                fuzzy search with 1 edit distance and 2 means allow fuzzy
        #                search with 2 edit distances
        #
        # prefix_length, this argument is valid only with fuzzy=(1 or 2). This
        #                controls the number of expansions allowed for
        #                fuzziness. Lower the value more number of expansions
        #                possible and thus is more time consuming
        #
        # max_expansions, this argument is valid only fuzzy. This controls the
        #                 maximum number of expansions allowed for a string

        return msg
    except KeyError as e:
        print(str(e))
        print("error")


with open("./test.txt") as inf:
    for ln in inf:
        # j = json.loads(ln)
        print(tmpfun(ln)['embersGeoCode'])
