#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

from geoutils.gazetteer import GeoNames
from geoutils.dbManager import ESWrapper
from geocode import BaseGeo

db = ESWrapper(index_name="geonames", doc_type="places")
GEO = BaseGeo(db)

def tmpfun(doc):
    try:
        msg = json.loads(doc)
        msg = GEO.annotate(msg)
        return msg
    except:
        print("error")


with open(infile) as inf:
    for ln in inf:
        j = json.loads(ln)
        tmpfun(doc)


