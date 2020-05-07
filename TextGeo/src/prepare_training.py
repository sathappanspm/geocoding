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
from geoutils import GeoPoint
from geoutils.gazetteer_mod import GeoNames
from geoutils.dbManager import ESWrapper
from collections import defaultdict
from urlparse import urlparse
from geoutils import LocationDistribution
import logging
from geoutils import encode, isempty
import json
import ipdb
import numpy as np
import re
from collections import Counter

numstrip=re.compile("\d")
tracer = logging.getLogger('elasticsearch')
tracer.setLevel(logging.CRITICAL)  # or desired level
tracer = logging.getLogger('urllib3')
tracer.setLevel(logging.CRITICAL)  # or desired level
# tracer.addHandler(logging.FileHandler('indexer.log'))
logging.basicConfig(filename='geocode.log', level=logging.DEBUG)
log = logging.getLogger("rssgeocoder")


class PrepareTraining(object):
    def __init__(self, db, min_popln=0, min_length=1):
        self.gazetteer = GeoNames(db)
        self.min_popln = min_popln
        self.min_length = min_length
        self.weightage = {
            "LOCATION": 1.0,
            "NATIONALITY": 0.75,
            "ORGANIZATION": 0.5,
            "OTHER": 0.0
        }

    def geocode(self, doc=None, loclist=None, eKey='BasisEnrichment', **kwargs):
        locTexts, persons = [], []
        NAMED_ENTITY_TYPES_TO_CHECK = [key for key in self.weightage if self.weightage[key] > 0]
        if doc is not None:
            doclength = len(doc[eKey]['tokens'])
            
            locTexts += [(numstrip.sub("", l['expr'].lower()).strip(),
                          l['neType'],
                          (sum([int(_) for _ in l['offset'].split(":")]))/(2.0 * doclength))
                         for l in
                         doc[eKey]["entities"]
                         if ((l["neType"] in NAMED_ENTITY_TYPES_TO_CHECK) and
                             len(l['expr']) >= self.min_length)]
            
            persons = [(numstrip.sub("", l['expr'].lower()).strip(),
                        (sum([int(_) for _ in l['offset'].split(":")]))/(2.0 * doclength))
                        for l in
                        doc[eKey]["entities"]
                        if ((l["neType"] == "PERSON") and
                        len(l['expr']) >= self.min_length)]

        if loclist is not None:
            locTexts += [l.lower() for l in loclist]

        return self.geocode_fromList(locTexts, persons, doclength=doclength, **kwargs)

    def geocode_fromList(self, locTexts, persons, results=None, min_popln=None, **kwargs):
        if results is None:
            results = {}

        if min_popln is None:
            min_popln = self.min_popln

        meta_entInfo = {}
        realized_countries = []
        idx = 0
        offsetmat = []
        for entitem in locTexts:
            querytext, enttype, offset = entitem
            if isempty(querytext):
                continue
            
            if querytext in results:
                results[querytext].frequency += 1
                meta_entInfo[querytext]["offsets"].append(offset)
                meta_entInfo[querytext]["neType"] = (enttype)
                meta_entInfo[querytext]["indexes"].append(idx)
                offsetmat.append(offset)
            else:
                for subidx, substr in enumerate(querytext.split(",")):
                    substr = substr.strip()
                    if substr in results:
                        results[substr].frequency += 1
                        meta_entInfo[substr]["offsets"].append(offset + float(subidx)/kwargs['doclength'])
                        meta_entInfo[substr]["neType"] = (enttype)
                        meta_entInfo[substr]["indexes"].append(idx + subidx)
                        offsetmat.append(offset + float(subidx)/kwargs['doclength'])
                        continue
                    
                    if substr not in meta_entInfo:
                        meta_entInfo[substr] = {"offsets": [offset + float(subidx)/kwargs['doclength']],
                                                "neType": enttype,
                                                "indexes": [idx + subidx]}   
                        offsetmat.append(offset + float(subidx)/kwargs['doclength'])
                    else:
                        meta_entInfo[substr]["offsets"].append(offset + float(subidx)/kwargs['doclength'])
                        meta_entInfo[substr]["neType"] = (enttype)
                        meta_entInfo[substr]["indexes"].append(idx + subidx)
                        offsetmat.append(offset + float(subidx)/kwargs['doclength'])
                        
                    ld = self._queryitem(substr, meta_entInfo[substr]["neType"])
                    if meta_entInfo[substr]["neType"] != "LOCATION" and ld.isempty():
                        continue
                    
                    results[substr] = ld
                    if len(results[substr].realizations) == 1:
                        realized_countries.append(list(results[substr].realizations.values())[0]['countryCode'].lower())
               
                    results[substr].frequency = 1
                idx += subidx 
            
            idx += 1
            
        
        offsetmat =  np.array(offsetmat)
        offset_diffmat = offsetmat[:, np.newaxis] - offsetmat
        realized_countries = Counter(realized_countries)
        co_realized = float(sum(realized_countries.values()))
        selco = [kl for kl, vl in realized_countries.viewitems()
                 if float(vl/co_realized) >= 0.5]
        try:
            selco = realized_countries.most_common(n=1)[0][0]
        except:
            selco = []

        persons_res = {}
        for entitem in persons:
            querytext, offset = entitem
            if querytext not in persons_res:
                diffs = offsetmat - offset                
                persons_res[querytext] = {"expansions": self._queryitem(querytext, "LOCATION", countryCode=selco),
                                          "offset": diffs, "freq": 1}
                
            else:
                persons_res[querytext]["freq"] += 1
        
        
        if not isempty(selco):
            results = self.fuzzyquery(results, 
                                      countryFilter=selco)
        
        freqsheet = self.score(results, meta_entInfo)

        return results, freqsheet, locTexts, meta_entInfo, offset_diffmat, persons_res
    
    def _queryitem(self, item, itemtype, **kwargs):
        if itemtype == "LOCATION": 
            res = self.gazetteer.query(item, **kwargs)
        else:
            res = self.gazetteer.query(item, fuzzy='AUTO', featureCode='pcli', operator='or')
            if res == []:
                res = self.gazetteer.query(item, featureCode='adm1', operator='or')
        
        return LocationDistribution(res)


    def fuzzyquery(self, locmap, countryFilter=[]):
        for loc in locmap:
            if len(locmap[loc].realizations) != 1:
                freq = locmap[loc].frequency
                subres = self.gazetteer.query(loc, countryCode=countryFilter, fuzzy='AUTO')
                if subres != []:
                    locmap[loc] = LocationDistribution(subres)
                    locmap[loc].frequency = freq
        return locmap
    
    
    def score(self, results, metaInfo):
        scoresheet = defaultdict(lambda : defaultdict(lambda : {"freq": 0.0, "offs_idx": []}))
        num_mentions = float(sum((l.frequency for l in results.values())))

        def update(key, l):
            offs = metaInfo[key]["indexes"]
            for s in l.city:
                scoresheet["city"][s]['freq'] += l.frequency
                scoresheet["city"][s]['offs_idx'] += (offs)
                
            for s in l.admin1:
                scoresheet["admin1"][s]["freq"] += l.frequency
                scoresheet["admin1"][s]['offs_idx'] += (offs)
                
            for s in l.country:
                scoresheet["country"][s]["freq"] += l.frequency
                scoresheet["country"][s]['offs_idx'] += (offs)

        _ = [update(key, val) for key, val in results.viewitems()]
        
        for typ in scoresheet:
            for s in scoresheet[typ]:
                scoresheet[typ][s]['freq'] /= num_mentions
                
            scoresheet[typ].default_factory = None
        
        scoresheet.default_factory = None
        return scoresheet

    def _builddoc(self, doc, enrichmentKeys=['BasisEnrichment'], **kwargs):
        """
        Attach embersGeoCode to document
        """
        eKey = None
        for key in enrichmentKeys:
            if key in doc and doc[key]:
                eKey = key

        if eKey is None:
            return doc

        all_exp_locs, freqsheet, loctexts, metaInfo, offsdiffmat, persons_res = self.geocode(doc)
        label_locs, Xmat, Ymat, pers_data, idxes = self.matchwithGSRLocs(doc, all_exp_locs, persons_res, offsdiffmat, freqsheet, metaInfo)
        doc['match_indexes'] = idxes 
        # Xmat, Ymat = [], []
        # for loc in all_exp_locs:
        #     if all_exp_locs[loc].haslabel is True:
        #         for r in all_exp_locs[loc].realizations.values():
        #             Ymat.append(r.label)
        #             Xmat.append(self.build_featuremat(r, offsdiffmat, freqsheet))
        
        # label_locs, freqsheet, loctexts, metaInfo, offsdiffmat,
        return doc
        # return Xmat, Ymat, pers_data
 
    def build_trainingdata(self, docs):
        xmat, ymat = [],[]
        for doc in docs:
            x, y = self._builddoc(doc)
            xmat += x
            ymat += y
            
        return xmat, ymat
    
    def calc_offset_stats(self, indices, diffmat):
        tril = np.tril(diffmat[indices])
        ntril = tril[np.nonzero(tril)]
        abstril = np.abs(ntril)
        if abstril.shape[0] == 0:
            return 1, 1, 1, 1
        
        
        abs_minval = np.min(abstril)
        medval = np.mean(abstril)
            
        
        try:
            before_closest = np.min(ntril[ntril > 0])
        except:
            before_closest = 1
            
        try:
            after_closest = abs(np.max(ntril[ntril < 0]))
        except:
            after_closest = 1
            
        return medval, abs_minval, before_closest, after_closest
    
    def _single_build_featuremat(self, realization, diffmat, freqsheet):
        country = realization.country
        admin = "/".join([country, realization.admin1])
        city = "/".join([admin, getattr(realization, "admin2", "") or realization.city])
        featureCode = realization.featureCode
        offs = freqsheet["country"][country+"//"]["offs_idx"]
        co_offset = self.calc_offset_stats(np.ix_(offs, offs), diffmat)
        
        offs = freqsheet["admin1"][admin+"/"]["offs_idx"]
        st_offset = self.calc_offset_stats(np.ix_(offs, offs), diffmat)
        if realization.featureCode[:3] not in ("adm1", "pcli"):
            try:
                offs = freqsheet["city"][city]["offs_idx"]
                ci_offset = self.calc_offset_stats(np.ix_(offs, offs), diffmat)
                cifreq = freqsheet["city"][city]["freq"]
            except:
                ci_offset = [1, 1, 1, 1]
                cifreq = 0
        else:
            ci_offset = [0, 0, 0, 0]
            cifreq = 0
            
        return {
            "country": freqsheet["country"][country+"//"]["freq"],
            "state": freqsheet["admin1"][admin+"/"]["freq"],
            "city": cifreq,
            "poplnConf": realization.poplnConf,
            "co_Offmean":co_offset[0],
            "co_Offmin": co_offset[1],
            "co_prev": co_offset[2],
            "co_after":co_offset[3],
            "st_offmean": st_offset[0],
            "st_offmin": st_offset[1],
            "st_prev": st_offset[2],
            "st_after": st_offset[3],
            "ci_offmean": ci_offset[0],
            "ci_offmin": ci_offset[1],
            "ci_prev": ci_offset[2],
            "ci_after": ci_offset[3]
        }
    
    def build_persmat(self, realization, meta_info, freqsheet):
        country = realization.country
        admin = "/".join([country, realization.admin1])
        city = "/".join([admin, getattr(realization, "admin2", "") or realization.city])
        featureCode = realization.featureCode
        co_offset = self.calc_offset_stats(freqsheet["country"][country+"//"]["offs_idx"], meta_info['offset'])
        
        if (admin + "/") in freqsheet["admin1"]:
            st_offset = self.calc_offset_stats(freqsheet["admin1"][admin+"/"]["offs_idx"], meta_info["offset"])
            st_freq = freqsheet["admin1"][admin + "/"]["freq"]
        else:
            st_offset = [1, 1, 1, 1]
            st_freq = meta_info['freq']
        
        if realization.featureCode[:3] not in ("adm1", "pcli"):
            if city in freqsheet["city"]:
                ci_offset = self.calc_offset_stats(freqsheet["city"][city]["offs_idx"], meta_info["offset"])
                cifreq = freqsheet["city"][city]["freq"]
            else:
                ci_offset = [1, 1, 1, 1]
                cifreq = meta_info["freq"]
        else:
            ci_offset = [0, 0, 0, 0]
            cifreq = 0
            
        return {
            "country": freqsheet["country"][country+"//"]["freq"],
            "state": st_freq,
            "city": cifreq,
            "poplnConf": realization.poplnConf,
            "co_Offmean":co_offset[0],
            "co_Offmin": co_offset[1],
            "co_prev": co_offset[2],
            "co_after":co_offset[3],
            "st_offmean": st_offset[0],
            "st_offmin": st_offset[1],
            "st_prev": st_offset[2],
            "st_after": st_offset[3],
            "ci_offmean": ci_offset[0],
            "ci_offmin": ci_offset[1],
            "ci_prev": ci_offset[2],
            "ci_after": ci_offset[3]
        }
        #self.build_persmat(persons_res[loc].realizations[x])
    
    def build_featuremat(self, loc, *args):
        xmat, ymat = [], []
        for real in loc.realizations:
            x = self._single_build_featuremat(loc.realizations[real], *args)
            y = loc.realizations[real].label
            xmat.append(x)
            ymat.append(y)
        
        return xmat, ymat
    
    def matchwithGSRLocs(self, doc, all_exp_locs, persons_res, offsdiffmat, freqsheet, metaInfo):
        locstrings = set()
        matched_idx = []
        for evt in doc['events']:
            estr = u"/".join([evt['Country'], evt['State'], evt['City'].lower()])
            locstrings.add(estr.lower())
            if 'expanded_loc' in evt:
                try:
                    loc = self.gazetteer.get_locInfo(country=evt['Country'],
                                                     admin=evt['State'],
                                                     city=evt["City"])
                    evt['expanded_loc'] = loc
                except Exception as e:
                    pass
                
            if "expanded_loc" in evt:
                for loc in evt['expanded_loc']:
                    gp = GeoPoint(**loc)
                    lstr = "/".join([gp.country, gp.admin1, (getattr(gp, "admin2", "") or gp.city)])
                    locstrings.add(lstr.lower())
            
        matched_locs = set()
        xmat, ymat = [], []
        for loc in all_exp_locs:
            all_exp_locs[loc].haslabel = False
            for x in all_exp_locs[loc].realizations:
                if x.lower() in locstrings:
                    all_exp_locs[loc].realizations[x].label = True
                    all_exp_locs[loc].haslabel = True
                    matched_locs.add(x.lower())
                    matched_idx.append([loc, metaInfo[loc]['indexes']])
                else:
                    all_exp_locs[loc].realizations[x].label = False
                    
            if all_exp_locs[loc].haslabel:
                x, y = self.build_featuremat(all_exp_locs[loc], offsdiffmat, freqsheet)
                xmat.append(x)
                ymat.append(y)
        
        remaininglocs = locstrings - matched_locs
        pers_data = [[], []]
        for loc in persons_res:
            persons_res[loc]['expansions'].haslabel = False
            for x in persons_res[loc]["expansions"].realizations:
                d1 = self.build_persmat(persons_res[loc]["expansions"].realizations[x],
                                       persons_res[loc], 
                                       freqsheet)
                pers_data[0].append(d1)
                if x.lower() in remaininglocs:
                    persons_res[loc]["expansions"].haslabel = True
                    persons_res[loc]["expansions"].realizations[x].label = True
                    matched_idx.append([loc, None])
                    pers_data[1].append(True)
                else:
                    pers_data[1].append(False)
                    
        return all_exp_locs, xmat, ymat, pers_data, matched_idx
        
        

def tmpfun(doc):
    try:
        msg = json.loads(doc)
        # x, y, pers = GEO._builddoc(msg)
        msg = GEO._builddoc(msg)
        return msg
        # return [x, y, pers]
    except Exception as e:
        print("error", str(e))


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

    db = ESWrapper(index_name="geonames2", doc_type="places2")
    GEO = PrepareTraining(db)

    if args.cat:
        infile = sys.stdin
        outfile = sys.stdout
    else:
        infile = smart_open(args.infile)
        outfile = smart_open(args.outfile, "wb")

    lno = 0
    wp = WorkerPool(infile, outfile, tmpfun, 100)
    wp.run()
    #import pdb
    #for l in infile:
    #    d = (tmpfun(l))
        
        # pdb.set_trace()


    # if not args.cat:
        # infile.close()
    #     outfile.close()

    exit(0)

                    
