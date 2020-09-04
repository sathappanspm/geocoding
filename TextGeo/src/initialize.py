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

from .geoutils.dbManager import ESWrapper
db = ESWrapper('geonames', 'places')

curr_dir = os.path.dirname(__file__)

def main(): 
    db.create("./gnames_gaz/allCountries.txt")


def create(admin1csv, admin2csv, countryCsv, confDir=os.path.join(curr_dir, "../data/")):
    with open(admin1csv) as acsv:
        with open(os.path.join(confDir, "geonames_admin1.conf")) as t:
            columns = [l.split(" ", 1)[0] for l in json.load(t)['columns']]

        admin1 = {}
        for l in acsv:
            if not l.startswith('#'):
                row = dict(list(zip(columns, l.strip().split("\t"))))
                admin1[row['key']] = row
                row['admin1'] = row['name']
                row['featureCode'] = 'ADM1'
                row['featureClass'] = 'A'
                row['population'] = db.getByid(row['geonameid'])['population']

        with open(os.path.join(curr_dir, "./geoutils/adminInfo.json"), "w") as ainf:
            ainf.write(json.dumps(admin1))

    with open(countryCsv) as ccsv:
        with open(os.path.join(confDir, "geonames_countryInfo.conf")) as t:
            columns = [l.split(" ", 1)[0] for l in json.load(t)['columns']]

        countryInfo = {}
        for l in ccsv:
            if not l.startswith('#'):
                row = dict(list(zip(columns, l.strip().split("\t"))))
                countryInfo[row['ISO']] = row
                row['featureCode'] = 'PCLI'
                row['featureClass'] = 'A'
                row['countryCode'] = row['ISO']
                row['name'] = row['country']

        with open(os.path.join(curr_dir, "./geoutils/countryInfo.json"), "w") as cinf:
            cinf.write(json.dumps(countryInfo))

    with open(admin2csv) as acsv:
        with open(os.path.join(confDir, "geonames_admin1.conf")) as t:
            columns = [l.split(" ", 1)[0] for l in json.load(t)['columns']]

        admin2 = {}
        for l in acsv:
            if not l.startswith('#'):
                row = dict(list(zip(columns, l.strip().split("\t"))))
                admin2[row['key']] = row
                row['featureCode'] = 'ADM2'
                row['featureClass'] = 'A'
                row['countryCode'], row['admin1'], row['admin2'] = row['key'].split(".")
                row['population'] = db.getByid(row['geonameid'])['population']

        with open(os.path.join(curr_dir, "./geoutils/Admin2Info.json"),  "w") as cinf:
            cinf.write(json.dumps(admin2))

    return


def download_gazeteer():
    os.makedirs('gnames_gaz')
    cwd = os.getcwd()
    os.chdir('./gnames_gaz')
    os.system('wget http://download.geonames.org/export/dump/admin2Codes.txt')
    os.system('wget http://download.geonames.org/export/dump/admin1CodesASCII.txt')
    os.system('wget http://download.geonames.org/export/dump/countryInfo.txt')
    os.system('wget http://download.geonames.org/export/dump/allCountries.zip')
    os.system('unzip allCountries.zip ')
    os.chdir(cwd)
    os.system("curl -XPUT 'localhost:9200/geonames' -H 'Content-Type: application/json' -d @{}".format(os.path.join(curr_dir, '../data/es_settings.json')))
    return

def run():
    download_gazeteer()
    main()
    create("./gnames_gaz/admin1CodesASCII.txt",
           "./gnames_gaz/admin2Codes.txt",
           "./gnames_gaz/countryInfo.txt")


if __name__ == '__main__':
    run()
    
