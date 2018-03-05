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

gn = GeoNames("./Geonames_dump.sql", "")
# with strict =False, only the country and city are matched (no state match is done..)
print gn.get_locInfo(country="Argentina", admin="", city="buenos aires", strict=False)[0].__dict__

print (gn.get_locInfo(country='Mexico', admin="", city=u"Ciudad de México", strict=False)[0].__dict__).__str__().encode("utf-8")



