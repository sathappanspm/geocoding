#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

import os
import json
import ipdb

from geoutils.dbManager import ESWrapper
db = ESWrapper('geonames2', 'places2')
db.create("./gnames_gaz/allCountries.txt")


def create(admin1csv, admin2csv, countryCsv, confDir="../data/"):
    with open(admin1csv) as acsv:
        with open(os.path.join(confDir, "geonames_admin1.conf")) as t:
            columns = [l.split(" ", 1)[0] for l in json.load(t)['columns']]

        admin1 = {}
        for l in acsv:
            if not l.startswith('#'):
                row = dict(zip(columns, l.strip().decode('utf-8').split("\t")))
                admin1[row['key']] = row
                row['admin1'] = row['name']
                row['featureCode'] = 'ADM1'
                row['featureClass'] = 'A'
                row['population'] = db.getByid(row['geonameid'])['population']

        with open("./geoutils/adminInfo.json", "w") as ainf:
            ainf.write(json.dumps(admin1, ensure_ascii=False).encode('utf-8'))

    with open(countryCsv) as ccsv:
        with open(os.path.join(confDir, "geonames_countryInfo.conf")) as t:
            columns = [l.split(" ", 1)[0] for l in json.load(t)['columns']]

        countryInfo = {}
        for l in ccsv:
            if not l.startswith('#'):
                row = dict(zip(columns, l.strip().decode('utf-8').split("\t")))
                countryInfo[row['ISO']] = row
                row['featureCode'] = 'PCLI'
                row['featureClass'] = 'A'
                row['countryCode'] = row['ISO']
                row['name'] = row['country']

        with open("./geoutils/countryInfo.json", "w") as cinf:
            cinf.write(json.dumps(countryInfo, ensure_ascii=False).encode('utf-8'))

    with open(admin2csv) as acsv:
        with open(os.path.join(confDir, "geonames_admin1.conf")) as t:
            columns = [l.split(" ", 1)[0] for l in json.load(t)['columns']]

        admin2 = {}
        for l in acsv:
            if not l.startswith('#'):
                row = dict(zip(columns, l.strip().decode('utf-8').split("\t")))
                admin2[row['key']] = row
                row['featureCode'] = 'ADM2'
                row['featureClass'] = 'A'
                row['countryCode'], row['admin1'], row['admin2'] = row['key'].split(".")
                row['population'] = db.getByid(row['geonameid'])['population']

        with open("./geoutils/Admin2Info.json",  "w") as cinf:
            cinf.write(json.dumps(admin2, ensure_ascii=False).encode('utf-8'))

    return

create("./gnames_gaz/admin1CodesASCII.txt",
       "./gnames_gaz/admin2Codes.txt",
       "./gnames_gaz/countryInfo.txt")

