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
from geoutils import LocationDistribution
from geoutils.dbManager import ESWrapper
from geoutils.gazetteer_mod import GeoNames
import re
from geoutils import FEATURE_WEIGHTS
import networkx as nx
from networkx.algorithms import approximation
from geoutils import encode, isempty
import json
from collections import defaultdict
import logging
from unidecode import unidecode

tracer = logging.getLogger('elasticsearch')
tracer.setLevel(logging.CRITICAL)  # or desired level
tracer = logging.getLogger('urllib3')
tracer.setLevel(logging.CRITICAL)  # or desired level
# tracer.addHandler(logging.FileHandler('indexer.log'))
logging.basicConfig(filename='steinerGeo.log', level=logging.DEBUG)
log = logging.getLogger("root")


numstrip = re.compile("\d")

FEATURE_WEIGHTS = {'adm1': 2, 'adm2': 3, 'adm3': 4, 'adm4': 4, 'ppla2': 4, 'ppla': 3,
                   'adm5': 4, 'pcli': 1, 'ppla3': 4, 'pplc': 3, 'ppl': 8, 'cont': 1}


class SteinerGeo():
    def __init__(self, db, nerKeyMap=None, spacy=False):
        self.gazetteer = GeoNames(db, confMethod='Uniform', escore=False)
        DEFAULT_NER_MAP = {'LOCATION': 'LOCATION', 'ORGANIZATION': 'ORGANIZATION',
                'NATIONALITY': 'NATIONALITY', 'OTHER': 'OTHER', 'PERSON': 'PERSON'}

        if nerKeyMap is None:
            nerKeyMap = DEFAULT_NER_MAP
        else:
            for key in DEFAULT_NER_MAP:
                if key not in nerKeyMap:
                    nerKeyMap[key] = DEFAULT_NER_MAP[key]

        if spacy is True:
            nerKeyMap['GPE'] = 'LOCATION'
            nerKeyMap['NORP'] = 'NATIONALITY'
            nerKeyMap['ORG'] = 'ORGANIZATION'
            nerKeyMap['LOC'] = 'LOCATION'

        self.nerKeyMap = nerKeyMap
        self.weightage = {
            "LOCATION": 1.0,
            "NATIONALITY": 0.75,
            "ORGANIZATION": 0.5,
            "OTHER": 0.0,
            "PERSON": 0.0
        }

    def geocode(self, doc):
        entities = defaultdict(list)
        NAMED_ENTITY_TYPES_TO_CHECK = [key for key in self.nerKeyMap if self.weightage[self.nerKeyMap[key]] > 0]
        _ = [entities[self.nerKeyMap[l['neType']]].extend((x.strip() for x in numstrip.sub("", l['expr']).split(",")))
             for l in doc['BasisEnrichment']['entities'] if (len(l['expr']) > 2) and (l['neType'] in NAMED_ENTITY_TYPES_TO_CHECK) ]

        idmap = {}
        cc = set()
        for loc in entities['LOCATION']:
            loc = loc.lower()
            if loc in idmap:
                idmap[loc]['count'] += 1
            else:
                expansions = self.gazetteer.query(loc)
                resolved = False
                if len(expansions) == 1:
                    resolved = True
                    cc.add(expansions[0].countryCode.lower())
                idmap[loc] = {'expansions': {exp.geonameid: exp for exp in expansions}, 'resolved': resolved,
                              'count': 1}

        # check if any organization is talking about a country
        organization_checklist = {}
        for org in (entities['ORGANIZATION'] + entities.get('NATIONALITY', [])):
            if org.isupper():
                continue

            org = org.lower()
            country = self.gazetteer.query(org, fuzzy='AUTO', featureCode='pcli', operator='or')
            if country:
                cc.add(country[0].countryCode.lower())
                if org in idmap:
                    idmap[org]['count'] += 1
                else:
                    idmap[org] = {'expansions': {exp.geonameid: exp for exp in country}, 'resolved': True,
                                  'count': 1}
            else:
                if org in organization_checklist:
                    organization_checklist[org] += 1
                else:
                    organization_checklist[org] = 1

        locdist = idmap
        if cc:
            locdist = self.fuzzyquery(idmap, organization_checklist, tuple(cc))
        #self.locdist = locdist
        #return locdist
        G, focus = self. steiner_tree_approx(locdist)
        return G, locdist, focus

    def annotate(self, doc):
        #stG, locdist
        stG, locdist, focus = self.geocode(doc)
        doc['location_distribution'] = {loc: locdist[loc]['expansions'][stG.neighbors(unidecode(loc+u"T0")).__next__()].__dict__ for loc in locdist if locdist[loc]['expansions']}
        if focus:
            doc['embersGeoCode'] = doc['location_distribution'][focus[0][:-2]]
        else:
            doc['embersGeoCode'] = {}

        self.graph = stG
        return doc

    def steiner_tree_approx(self, locationMap):
        G = nx.DiGraph()
        terminalNodes = ["E"]
        for loc in locationMap:
            for rl in locationMap[loc]['expansions'].values():
                #eW = (2 - FEATURE_WEIGHTS.get(rl.featureCode, 0.00))
                eW = FEATURE_WEIGHTS.get(rl.featureCode, 12)
                nodename = unidecode(loc + u"T0")
                if rl.ltype == 'country':
                    edges = [(nodename, rl.geonameid, eW), (rl.geonameid, rl.country, eW), (rl.country, 'E', eW)]
                elif rl.ltype == 'admin1':
                    edges =  [(nodename, rl.geonameid, eW), (rl.geonameid, rl.admin1, eW), (rl.admin1, rl.country, eW), (rl.country, 'E', eW)]
                else:
                    #edges = [(loc + "T0", rl.geonameid, eW), (rl.geonameid, rl.name, eW), (rl.name, rl.admin1, eW), (rl.admin1, rl.country, eW), (rl.country, 'E', eW)]
                    edges = [(nodename, rl.geonameid, eW), (rl.geonameid, rl.admin1, eW), (rl.admin1, rl.country, eW), (rl.country, 'E', eW)]

                G.add_weighted_edges_from(edges)
                terminalNodes.append(nodename)

        if G.number_of_nodes() == 0:
            return G, []

        stG = approximation.steiner_tree(G.to_undirected(), terminalNodes)
        def ego_nw_degree(degree, node):
            return sum((degree(p) for p in nx.descendants(G, node)))

        G = G.subgraph(stG)
        degree = G.degree()
        geofocus = sorted([(t, ego_nw_degree(degree, t)) for t in terminalNodes[1:]], reverse=True)
        return G, geofocus[0] if geofocus else []

    def fuzzyquery(self, locmap, orgChecklist, countryFilter=[]):
        for loc in locmap:
            if locmap[loc]['resolved'] is False:
                subres = self.gazetteer.query(loc, countryCode=countryFilter, fuzzy='AUTO')
                new_exp = {res.geonameid: res for res in subres}
                if new_exp:
                    # locmap[loc]['expansions'].update(new_exp)
                    locmap[loc]['expansions'] = (new_exp)

        for org in orgChecklist:
            subres = self.gazetteer.query(org, countryCode=countryFilter)
            locmap[org] = {"expansions": {res.geonameid: res for res in subres},
                           "resolved": len(subres) == 1,
                           "count": orgChecklist[org]
                           }
        return locmap


def tmpfun(doc):
    try:
        msg = json.loads(doc)
        msg = GEO.annotate(msg)
        return msg
    except Exception as e:
        print(e)
        print("error")


if __name__ == "__main__":
    import sys
    import argparse
    from geoutils import smart_open
    parser = argparse.ArgumentParser()
    parser.add_argument("--cat", "-c", action='store_true',
                        default=False, help="read from stdin")
    parser.add_argument("-i", "--infile", type=str, help="input file")
    parser.add_argument("-o", "--outfile", type=str, help="output file")

    args = parser.parse_args()

    db = ESWrapper(index_name="geonames", doc_type="places")
    GEO = SteinerGeo(db)

    if args.cat:
        infile = sys.stdin
        outfile = sys.stdout
    else:
        infile = smart_open(args.infile)
        outfile = smart_open(args.outfile, "wb")

    lno = 0
    wp = WorkerPool(infile, outfile, tmpfun, 200, limit=1000)
    wp.run()
    #for l in infile:
    #    tmpfun(l)
