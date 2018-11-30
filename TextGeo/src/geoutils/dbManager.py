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
import re
#from copy import deepcopy
from .loc_config import stop_words as STOP_WORDS
from unidecode import unidecode

unicodecsv.field_size_limit(sys.maxsize)

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

tokenizer = re.compile(u' |\||,|_|-')

class BaseDB(object):
    def __init__(self, dbpath):
        pass

    def insert(self, keys, values, **args):
        pass


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

    def _query(self, qkey, **kwargs):
        q = {"query": {"bool": {}}}
        query_name = "should"
        q["query"]["bool"]["minimum_number_should_match"] = 1
        kwargs.pop("qtype", "")

        placetokens = [l.strip() for l in tokenizer.split(qkey) if l and l not in STOP_WORDS and l[-1] != '.']
        if placetokens:
            reduced_placename = u" ".join(placetokens[0:])
            if len(placetokens[0]) < 3 and len(placetokens) > 1 and 3.0 / len(placetokens) >= .5:
                reduced_placename = u" ".join(placetokens[1:])
        else:
            reduced_placename = qkey

        # print "qkey", qkey, "reduced", reduced_placename
        maincondition = [
            {"bool": {"must": [{"multi_match": {"query": qkey, "fields": ["name.raw^5", "asciiname^5", "alternatenames"], "operator": "and"}},
                               {"terms": {"featureClass": ["a", "p"]}}],
                      }},
             {"term": {"name.raw": {"value": qkey}}},
             {"term": {"asciiname.raw": {"value": qkey}}},
             {"term": {"normalized_asciiname": {"value": qkey}}},
             # {"term": {"alternatenames": {"value": qkey[1:]}}},
             {"term": {"alternatenames": {"value": qkey}}},
             # {"multi_match": {"query": reduced_placename if 'fuzzy' in kwargs else unicode(unidecode(reduced_placename)),
                              
             {"multi_match": {"query": reduced_placename if 'fuzzy' in kwargs else unicode(unidecode(reduced_placename)),
                             'fuzziness': kwargs.pop("fuzzy", 0),
                             "max_expansions": kwargs.pop("max_expansion", 10),
                             "prefix_length": kwargs.pop("prefix_length", 1),
                             'operator': kwargs.pop("operator", "and"),
                             "fields": ["name^3", "asciiname^3", "alternatenames", "normalized_asciiname^3"]}}

          ]

        q["query"]["bool"][query_name] = maincondition

        if kwargs:
            filter_cond = []
            if 'min_popln' in kwargs:
                popln = kwargs.pop("min_popln")
                if popln is not None:
                    filter_cond.append({"range": {"population": {"gte": popln}}})

            for key, val in kwargs.viewitems():
                if not isinstance(val, basestring):
                    val = list([(v) for v in val])
                    filter_cond.append({"terms": {key: val}})
                else:
                    filter_cond.append({"term": {key: (val)}})

            q["query"]["bool"]["filter"] = {"bool": {"must": filter_cond}}

        q['from'] = 0
        q['size'] = 50
        return self.eserver.search(q, index=self._index, doc_type=self._doctype)

    def query(self, qkey, min_popln=None, **kwargs):
        #res = self._query(qkey, min_popln=min_popln, **kwargs)['hits']['hits']
        res = self._query(qkey, min_popln=min_popln, **kwargs)['hits']
        #max_score = sum([r['_score'] for r in res])
        max_score =  res['max_score']#sum([r['_score'] for r in res])
        #for t in res:
        # print(max_score)
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

    def _oldquery(self, qkey, qtype="exact", analyzer=None, min_popln=None, size=10, **kwargs):
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

    def oldquery(self, qkey, min_popln=None, **kwargs):
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
                                      # {"featureCode":
                                      #  ["pcli", "ppl", "ppla2", "adm3"]}
                                      {"featureClass":
                                       ["a", "h", "l", "t", "p", "v"]}
                                     }
                                 ]
                                }
                      },
                      "sort": {"population": "desc"}
            }
        if kwargs:
            for key in kwargs:
                q2['query']['bool']['filter'].append({"term":{key: kwargs[key]}})

        res = self.eserver.search(q2, index=self._index,
                                  doc_type=self._doctype)['hits']['hits'][0]['_source']
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
        ere = re.compile("[^\sa-zA-Z0-9]")
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
                    row['alternatenames'] = row['alternatenames'].lower().split(",")
                    row['normalized_asciiname'] = (re.sub(r'\s+', r' ', ere.sub("", row['asciiname']))).strip()
                    cnt += 1
                    yield self.eserver.index_op(row, index=self._index, doc_type=self._doctype)
                except:
                    print json.dumps(row)
                    continue
                    
    def remove_dynamic_stopwords(self, term):
        # cc = {}
        # ttl = 0
        words = [w for t in term.split("-") for w in t.split() if len(w) > 1]
        
        if len(words) == 1:
            return term
        
        stopword_removed = ""
        for word in words:
            try:
                t = self.eserver.count(word)['count']
                if t >= 20000:
                    continue
            except:
                pass
            
            stopword_removed += (word + " ")
            # else:
            #     print(term, "stopword ", word)
        
        return stopword_removed.strip()
        


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
