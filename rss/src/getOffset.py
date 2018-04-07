#!/usr/bin/env python
#-*- coding:utf-8 -*-

"""
    *.py: Description of what * does.
    Last Modified:
"""

__author__ = "Sathappan Muthiah"
__email__ = "sathap1@vt.edu"
__version__ = "0.0.1"

import json
import ipdb

def get_locations(msg):
    return [l for l in msg['BasisEnrichment']['entities'] if l['neType'] == 'LOCATION']

def get_offsets(msg):
    loc_ents = get_locations(msg)
    lexpr = [l['expr'].lower() for l in loc_ents]
    ldist = msg['location_distribution']
    for l in ldist:
        FLAG = False
        ldist[l]['offset'] = []
        if 'URL' in l:
            ldist[l]['offset'] = ['url']
            continue
        for i, s in enumerate(lexpr):
            if l in s:
                FLAG = True
                ldist[l]['offset'].append(loc_ents[i]['offset'])

        if not FLAG:
            ipdb.set_trace()
            print msg['embersId']
            exit(0)
    return


with open("./autogsr_highlight_embers.json") as inf:
    with open("./autogsr/autogsr_english_highlight_geocoded.json", "w") as out:
        for l in inf:
            j = json.loads(l)
            get_offsets(j)
            out.write(json.dumps(j, ensure_ascii=False).encode("utf-8") + "\n")
