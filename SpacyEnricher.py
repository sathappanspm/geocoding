#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

import spacy
import json
#import gzip
from genericUtils import smart_open
from itertools import tee
from joblib import Parallel, delayed
import langdetect


class Enricher(object):
    def __init__(self, lang='en'):
        pass

    def enrich(self, msg):
        pass


class SpacyEnricher(Enricher):
    def __init__(self, lang='en', model=None):
        self.engine = spacy.load(lang)
        self.lang = lang
        self.to_tokendict = lambda x: {'POS': x.pos_, 'lemma': x.lemma_,
                                       'value': x.orth_} #'start': x.start_char,
                                       #'end': x.start_char + len(x)}
        self.to_entitydict = lambda x: {'expr': x.orth_, 'neType': x.label_,
                                        'offset': '{}:{}'.format(x.start, x.end)}

    def enrich(self, edoc):
        nlpdict = {"tokens": [], }
        for sent in edoc.sents:
            tlist = ([self.to_tokendict(token) for token in sent] +
                     [{"POS": "SENT", "lemma": ".", "value": "."}])
            nlpdict['tokens'] += tlist

        nlpdict['entities'] = [self.to_entitydict(ent) for ent in edoc.ents]
        return nlpdict

    def _reader(self, fname, contentKey):
        with smart_open(fname) as inf:
            for ln in inf:
                try:
                    j = json.loads(ln)
                    if j[contentKey]:
                        lang = langdetect.detect(j[contentKey])
                        if lang == self.lang:
                            yield j

                except Exception as e:
                    print(str(e))

    def run(self, fname, outfname, contentKey='text'):
        dataiter, textiter = tee(self._reader(fname, contentKey))
        with smart_open(outfname, "wb") as outf:
            lno = 0
            for jmsg, doc in zip(dataiter,
                                 self.engine.pipe((t[contentKey] for t in textiter),
                                                  n_threads=4, batch_size=100)):
                lno += 1
                try:
                    jmsg['BasisEnrichment'] = self.enrich(doc)
                    outf.write((json.dumps(jmsg, ensure_ascii=False) + "\n").encode("utf-8"))
                except Exception as e:
                    print(str(e), fname, lno)
                    pass
        print(lno)
        return

def single_run(fname, outfname):
    enricher = SpacyEnricher()
    enricher.run(fname, outfname, contentKey="text")
    return


if __name__ == "__main__":
    import argparse
    import glob
    import os
    ap = argparse.ArgumentParser()
    ap.add_argument("--indir", type=str, help="in directory")
    ap.add_argument("--outdir", type=str, help="out directory")
    args = ap.parse_args()

    _input = []
    for fl in glob.glob('{}*'.format(args.indir)):
        outname = os.path.join(args.outdir, os.path.basename(fl))
        _input.append((fl, outname))
        #nlp.run(fl, outname)
        #break

    Parallel(n_jobs=len(_input), backend='multiprocessing')(delayed(single_run)(*(item))
            for item in _input)
