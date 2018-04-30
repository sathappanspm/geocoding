#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

import ipdb
#import csv as unicodecsv
import unicodecsv
import sqlite3
#import gsqlite3 as sqlite3
import sys
import os
# from time import sleep
from . import GeoPoint, safe_convert, isempty
import json
from pyelasticsearch import bulk_chunks, ElasticSearch
import gzip
import zipfile
#from copy import deepcopy


unicodecsv.field_size_limit(sys.maxsize)

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"


sqltypemap = {'char': unicode,
              'BIGINT': int,
              'text': unicode, 'INT': int,
              'FLOAT': float, 'varchar': unicode
              }


class BaseDB(object):
    def __init__(self, dbpath):
        pass

    def insert(self, keys, values, **args):
        pass


class SQLiteWrapper(BaseDB):
    def __init__(self, dbpath, dbname="WorldGazetteer"):
        self.dbpath = dbpath
        self.conn = sqlite3.connect(dbpath, check_same_thread=False)
        self.conn.execute('PRAGMA synchronous = OFF')
        self.conn.execute('PRAGMA journal_mode = OFF')
        self.conn.execute("PRAGMA cache_size=5000000")
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        self.name = dbname

    def query(self, stmt=None, params=None):
        # self.conn = sqlite3.connect(self.dbpath)
        cursor = self.conn.cursor()
        cursor.row_factory = sqlite3.Row
        # ipdb.set_trace()
        # result = self.cursor.execute(stmt, params)
        result = cursor.execute(stmt, params)
        result = [GeoPoint(**dict(l)) for l in result.fetchall()]
        # result = [GeoPoint(**dict(l)) for l in result.fetchmany(1000)]
        # conn.close()
        return result

    def insert(self, msg, dup_id):
        return

    # outdated function
    def _create(self, csvPath, columns, delimiter="\t", coding='utf-8', **kwargs):
        with open(csvPath, "rU") as infile:
            infile.seek(0)
            reader = unicodecsv.DictReader(infile, dialect='excel',
                                           fieldnames=columns, encoding=coding)
            items = [r for r in reader]
            self.cursor.execute('''CREATE TABLE WorldGazetteer (id float,
                                    name text, alt_names text, orig_names text, type text,
                                    pop integer, latitude float, longitude float, country text,
                                    admin1 text, admin2 text, admin3 text)''')
            self.cursor.executemany('''INSERT INTO WorldGazetteer
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                    [tuple([i[c] for c in columns]) for i in items])
            self.conn.commit()

    def create(self, csvfile, fmode='r', delimiter='\t',
               coding='utf-8', columns=[], header=False, **kwargs):
        with open(csvfile, fmode) as infile:
            infile.seek(0)
            if header:
                try:
                    splits = infile.next().split(delimiter)
                    if not columns:
                        columns = splits
                except Exception:
                    pass

            self.cursor.execute(''' CREATE TABLE IF NOT EXISTS {} ({})'''.format(self.name,
                                                                                 ','.join(columns)))

            reader = infile
            columnstr = ','.join(['?' for c in columns])

            self.cursor.executemany('''INSERT INTO {}
                                    VALUES ({})'''.format(self.name, columnstr),
                                    (tuple([i for i in c.decode("utf-8").split("\t")])
                                     for c in reader)
                                    )
            # (tuple([c[i] for i in columns]) for c in reader))
            self.conn.commit()
            if 'index' in kwargs:
                for i in kwargs['index']:
                    col, iname = i.split()
                    self.cursor.execute('''CREATE INDEX IF NOT
                                           EXISTS {} ON {} ({})'''.format(iname.strip(),
                                                                          self.name,
                                                                          col.strip()))
                    self.conn.commit()
            self.conn.commit()

    def close(self):
        self.conn.commit()

    def drop(self, tablename):
        self.cursor.execute("DROP TABLE IF EXISTS {}".format(tablename))
        self.conn.commit()


class DataReader(object):
    def __init__(self, dataFile, configFile):
        self.columns = self._readConfig(configFile)
        if ".gz" in dataFile:
            self.dataFile = gzip.open(dataFile)
        elif '.zip' in dataFile:
            self.dataFile = zipfile.ZipFile(dataFile).open('allCountries.txt')
            self.dataFile.next = self.dataFile.readline
        else:
            self.dataFile = open(dataFile)
            #raise Exception('Unknown data file format')
        #self.dataFile = open(dataFile)

    def _readConfig(self, cfile):
        with open(cfile) as conf:
            cols = [l.split(" ", 1)[0] for l in json.load(conf)['columns']]

        return cols

    def next(self):
        row = dict(zip(self.columns, self.dataFile.next().decode("utf-8").strip().split("\t")))
        return row

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.close()

    def close(self):
        self.dataFile.close()


class ESWrapper(BaseDB):
    def __init__(self, index_name, doc_type, host='http://localhost', port=9200):
        self.eserver = ElasticSearch(urls=host, port=port,
                                     timeout=60, max_retries=3)
        #self._base_query = {"query": {"bool": {"must": {"match": {}}}}}
        #self._base_query = {"query": {"bool": {}}}
        self._geo_filter = {"distance": "20km", "coordinates": {}}
        self._population_filter = {'population': {'gte': 5000}}
        self._index = index_name
        self._doctype = doc_type

    def getByid(self,geonameId):
        maincondition = {"match": {"id": geonameId}}
        q = {"query": {"bool": {"must": maincondition}}}
        return self.eserver.search(q, index=self._index, doc_type=self._doctype)['hits']['hits'][0]['_source']

    def _query(self, qkey, qtype="exact", analyzer=None, min_popln=None, size=10, **kwargs):
        """
        qtype values are exact, relaxed or geo_distance
        Always limit results to 10
        """
        q = {"query": {"bool": {}}}
        query_name = kwargs.pop('query_name', 'must')
        query_name = "should"
        if query_name == "should":
            q["query"]["bool"]["minimum_number_should_match"] = 1

        maincondition = {}
        if qtype == "exact":
            maincondition = [{"term": {"name.raw": {"value": qkey}}},
                             {"term": {"asciiname.raw": {"value": qkey}}},
                             {"term": {"alternatenames": {"value": qkey}}}
                             ]
            #maincondition["match"] = {"name.raw": {"query": qkey}}
            if analyzer:
                maincondition["match"]["name.raw"]["analyzer"] = analyzer

        elif qtype == "relaxed":
            maincondition["match"] = {"alternatenames": {"query": qkey}}
            if analyzer:
                maincondition["match"]["alternatenames"]["analyzer"] = analyzer

            #q["query"]["bool"][query_name]["match"].pop("name.raw", "")
        elif qtype == "combined":
            maincondition = [
                {"bool": {"must": {"multi_match": {"query": qkey, "fields": ["name.raw", "asciiname", "alternatenames"]}},
                           "filter": {"bool": {"should": [{"range": {"population": {"gte": 5000}}},
                                                      {"terms": {"featureCode": ["pcla", "pcli", "cont",
                                                                                 "rgn", "admd", "adm1", "adm2"]}}]}}}},
                 {"term": {"name.raw": {"value": qkey}}},
                 {"term": {"asciiname.raw": {"value": qkey}}},
                 {"term": {"alternatenames": {"value": qkey[1:]}}},
                 {"match": {"alternatenames": {"query": qkey,
                                               'fuzziness': kwargs.pop("fuzzy", 0),
                                               "max_expansions": kwargs.pop("max_expansion", 5),
                                               "prefix_length": kwargs.pop("prefix_length", 1)}}}

              ]

        if maincondition:
            q["query"]["bool"][query_name] = maincondition

            if min_popln:
                filter_cond = [{"range": {"population": {"gte": min_popln}}}]
            else:
                filter_cond = []

            if kwargs:
                #filter_cond = [{"range": {"population": {"gte": min_popln}}}]
                filter_cond += [{"term": {key:val}} for key, val in kwargs.viewitems()]
                # print(filter_cond)
                q["query"]["bool"]["filter"] = {"bool": {"must": filter_cond}}
            elif min_popln:
                filter_cond = [{"range": {"population": {"gte": min_popln}}},
                                {"terms": {"featureCode": ["ppla", "pplx"]}}]

                q["query"]["bool"]["filter"] = {"bool": {"should": filter_cond}}

        return self.eserver.search(q, index=self._index, doc_type=self._doctype)

    def query(self, qkey, min_popln=None, **kwargs):
        #res = self._query(qkey, min_popln=min_popln, **kwargs)['hits']['hits']
        res = self._query(qkey, min_popln=min_popln, **kwargs)['hits']
        #max_score = sum([r['_score'] for r in res])
        max_score =  res['max_score']#sum([r['_score'] for r in res])
        #for t in res:
        gps = []
        if max_score == 0.0:
            ## no results were obtained by elasticsearch instead it returned a random/very
            ## low scoring one
            res['hits'] = []

        for t in res['hits']:
            t['_source']['geonameid'] = t["_source"]["id"]
            #t['_source']['_score'] = t[1] / max_score
            t['_source']['_score'] = t['_score'] / max_score
            pt = GeoPoint(**t["_source"])
            if t['_source']['featureCode'].lower() == "cont":
                gps = [pt]
                break

            gps.append(pt)

        if len(gps) == 1:
            gps[0]._score = (min(float(len(gps[0].name)),float(len(qkey)))
                                /max(float(len(gps[0].name)),float(len(qkey))))

        return gps

    def near_geo(self, geo_point, min_popln=5000, **kwargs):
        q2 = {"query": {"bool" : {"must" : {"match_all" : {}},
                                 "filter" : [{
                                     "geo_distance" : {
                                         "distance" : "30km",
                                         "coordinates" : geo_point
                                     }
                                 },
                                     {"terms":
                                      {"featureCode":
                                       ["pcli", "ppl", "ppla2", "adm3"]}
                                     }
                                 ]
                                }
                      },
                      "sort": {"population": "desc"}
            }

        res = self.eserver.search(q2, index=self._index,
                                  doc_type=self._doctype, **kwargs)['hits']['hits'][0]['_source']
        res['confidence'] = 1.0
        return [GeoPoint(**res)]

    def create(self, datacsv, confDir="../data/"):
        with open(os.path.join(confDir, "es_settings.json")) as jf:
            settings = json.load(jf)
            settings['mappings'][self._doctype] = settings['mappings'].pop('places')

        try:
            self.eserver.create_index(index=self._index, settings=settings)
        except:
            self.eserver.delete_index(self._index)
            self.eserver.create_index(index=self._index, settings=settings)

        for chunk in bulk_chunks(self._opLoader(datacsv, confDir),
                                 docs_per_chunk=1000):
            self.eserver.bulk(chunk, index=self._index, doc_type=self._doctype)
            print "..",

        self.eserver.refresh(self._index)

    def _opLoader(self, datacsv, confDir):
        with DataReader(datacsv, os.path.join(confDir, 'geonames.conf')) as reader:
            cnt = 0
            for row in reader:
                try:
                    row['coordinates'] = [float(row['longitude']), float(row['latitude'])]
                    try:
                        row['population'] = int(row["population"])
                    except:
                        row['population'] = -1

                    try:
                        row['elevation'] = int(row['elevation'])
                    except:
                        row['elevation'] = -1

                    del(row['latitude'])
                    del(row['longitude'])
                    #print row['name']
                    row['alternatenames'] = row['alternatenames'].split(",")
                    cnt += 1
                    yield self.eserver.index_op(row, index=self._index, doc_type=self._doctype)
                except:
                    print json.dumps(row)
                    continue


def main(args):
    """
    """
    import json
    with open(args.config) as cfile:
        config = json.load(cfile)

    fmode = config['mode']
    separator = config['separator']
    columns = config['columns']
    coding = config['coding']
    dbname = args.name
    infile = args.file
    index = config['index']
    sql = SQLiteWrapper(dbpath="./GN_dump_20160407.sql", dbname=dbname)
    if args.overwrite:
        sql.drop(dbname)
    sql.create(infile, mode=fmode, columns=columns,
               delimiter=separator, coding=coding, index=index)

    sql.close()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--name", type=str, help="database name")
    ap.add_argument("-c", "--config", type=str, help="config file containing database info")
    ap.add_argument("-f", "--file", type=str, help="csv name")
    ap.add_argument("-o", "--overwrite", action="store_true",
                    default=False, help="flag to overwrite if already exists")
    args = ap.parse_args()
    main(args)
