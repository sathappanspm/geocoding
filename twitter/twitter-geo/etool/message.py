#!/usr/bin/env python

import re
import json
import logging
import hashlib
import dateutil.parser
import datetime
import copy
from collections import Counter

log = logging.getLogger(__name__)
 
def add_embers_ids(obj, parent_id=None, derived_ids=None, feed=None, feedPath=None, resources=None, lang_loc=None):

    if parent_id and derived_ids:
        log.warning("Both parent_id and derived_ids given")

    if parent_id:
        obj['parentId'] = parent_id

    if derived_ids:
        if isinstance(derived_ids, basestring):
            derived_ids = [derived_ids]
        
        if not isinstance(derived_ids, list):
            log.error("Incorrect format of derived_ids, must be array or string, ignoring")
        else:
            obj2 = {}
            obj2['derivedIds'] = derived_ids
            obj['derivedFrom'] = obj2

    if feedPath:
        if 'feedPath' not in obj:
            if not isinstance(feedPath,list):
                 obj['feedPath'] = [feedPath]
            else:
                 obj['feedPath'] = copy.copy(feedPath)
        else:
            if not isinstance(feedPath,list):
                 obj['feedPath'].append(feedPath)
            else:
                 obj['feedPath'] = obj['feedPath'] + copy.copy(feedPath)

    if resources:
        if not isinstance(resources,list):
            obj['resources'] = [resources]
        else:
            obj['resources'] = copy.copy(resources)

    if feed:
        if 'feedPath' not in obj:
            obj['feedPath'] = []

        if isinstance(feed,list):
            feed = ' '.join(feed)

        obj['feed'] = feed
        obj['feedPath'].append(feed)
    
    if lang_loc:
        obj = normalize_language(obj, lang_loc)

    '''
    Create an EMBERS identifier for an object if none exists. 
    The id is just a SHA1 hash of the object content.
    '''
    if not obj.has_key('embersId'):
        obj['embersId'] = hashlib.sha1(str(obj)).hexdigest()

    return obj


def update_child_embers_id(obj, feed=None):
    '''
    Update embers ID for a child message (derived from obj)
    '''
    if 'embersId' not in obj:
        return add_embers_ids(obj, feed=feed)
    else:
        parent_id = obj['embersId']
        del obj['embersId']
        return add_embers_ids(obj, parent_id=parent_id, feed=feed)


##
## support for parsing 822 style dates with non-English day and month names
##
MONTH_NAMES = {
    # por
    "jan": 1, 	
    "fev": 2,	
    "marco": 3,
    "mar": 3,
    "abril": 4,
    "abr": 4,
    "maio": 5,
    "mai": 5,
    "junho": 6,
    "jun": 6,
    "julho": 7,
    "jul": 7,
    "agosto": 8,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
    # spa
    "enero":1,
    "ene":1,
    "feb": 2,
    "marzo": 3,
    "mar": 3,
    "abr": 4,
    "mayo": 5,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "julio": 7,
    "agosto": 8,
    "ago": 8,
    "sept": 9,
    "sep": 9,
    "set": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
    # spa full names
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7, 
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9, 
    "octubre": 10, 
    "noviembre": 11, 
    "diciembre": 12,
    # por full names
    "janeiro": 1, 
    "fevereiro": 2, 
    "marco": 3, 
    "abril": 4, 
    "maio": 5, 
    "junho": 6, 
    "julho": 7, 
    "agosto": 8, 
    "setembro": 9, 
    "outobro": 10, 
    "novembro": 11, 
    "dezembro": 12
}

# Country codes of "Saudi Arabia" "Kuwait" "Egypt" "Jordan" "Iraq" "Libya" "Bahrain"
MENA_LIST = ['SA', 'KW', 'EG', 'JO', 'IQ', 'LY', 'BH']

def parse_datetime(value):
    try:
        return dateutil.parser.parse(value)
    except ValueError:
        pass

    # for doing unicode matches
    if not isinstance(value, unicode):
        value = unicode(value, 'utf8', 'replace')

    # for RFC822-ish dates with Spanish and Portugese month names
    # e.g. Sex, 31 Mai 2013 10:44:00 GMT
    m = re.match(r'^\w+, (\d+) (\w+) (\d+) (\d+):(\d+):(\d+) (.*)', value, re.UNICODE)
    if m:
        # day, month, year, hour, minute, second, timezone
        (d, mn, y, hh, mm, ss, tz) = m.groups()
        mon = MONTH_NAMES.get(mn.lower())       
        if mon:
            # ignore timezones for now, the formats are too weird
            # and pytz doesn't have a way to cope with offsets 
            # (e.g. -0300) and most of them are UTC anyway
            return datetime.datetime(int(y), int(mon), int(d), int(hh), int(mm), int(ss))

    # some weirdo ISO-like dates with no date/time separation
    # e.g. 2011-07-2600:00:00
    m = re.match(r'^(\d+)-(\d+)-(\d{2})(\d+):(\d+):(\d+)', value)
    if m:
        (y, m, d, hh, mm, ss) = m.groups()
        return datetime.datetime(int(y), int(m), int(d), int(hh), int(mm), int(ss))

    # another odd format that appears in the data
    # e.g. "12 Ago 2011"
    m = re.match(r'^(\d+) (\w+) (\d+)', value, re.UNICODE)
    if m:
        (d, mn, y) = m.groups()
        mon = MONTH_NAMES.get(mn.lower())       
        if mon:
            return datetime.datetime(int(y), mon, int(d))

    return None


def normalize_date(obj, path):
    '''
    Find a date field in an object and convert it to
    a UTC ISO formatted date and write it back as 
    the 'date' field of the object.
    path - a field name, or array of field names describing the path to the source field.
    '''
    if obj.has_key('date'):
        dateStr = obj.get('date', None)
        if not dateStr is None:
            return obj

    value = None
    if isinstance(path, basestring):
        value = obj.get(path, None)
    else:
        tmp = obj
        for p in path:
            if isinstance(tmp, dict):
                tmp = tmp.get(p, None) 
        
        value = tmp

    result = None
    if isinstance(value, basestring):
        try:
            dt = parse_datetime(value)
            tt = dt.utctimetuple()
            # this is painful, but the only way I could figure to normalize the date
            # naive dates (e.g. datetime.now()) will have no conversion
            dt = datetime.datetime(*tt[0:6])
            result = dt.isoformat()
        except Exception as e:
            log.exception('Could not parse date "%s"', value)

    if isinstance(value, (int, float)):
        try:
            dt = datetime.datetime.utcfromtimestamp(value)
            result = dt.date().isoformat()
        except:
            log.exception('Could not parse date "%f"', value)
        
    if not result:
        result = datetime.datetime.utcnow().isoformat()
        
    obj['date'] = result
    return obj


def is_us_tweet(tweet):
    '''
    Detect tweets with US place marks.
    Currently just uses the 'place' indicator from twitter.
    Should tolerate Datasift or public API tweets. 
    (Datasift embeds the tweet in the 'tweet' field)
    '''
    n = tweet
    for k in ['twitter', 'place', 'country_code']:
        if n and k in n:
            n = n[k]

    return n == 'US'

def is_mena_tweet(tweet):
    """
    Check if the tweet from MENA countries.
    """
    n = tweet
    for k in ['twitter', 'place', 'country_code']:
        if n and k in n:
            n = n[k]

    return n in MENA_LIST

def clean(msg):
    """Take a string that contains a JSON message and fix all of the odd bits in it.
    Throw exceptions if it doesn't go well."""
    assert isinstance(msg, dict), "Message must be a python dictionary."

    if 'date' not in msg:
        if msg.get('interaction', {}).get('created_at'):
            msg = normalize_date(msg, ["interaction", "created_at"])
        elif msg.get('created_at'):
            msg = normalize_date(msg, ["created_at"])
        else:
            msg = normalize_date(msg, 'published')

    # legacy messages
    if msg.get('embers_id') and not msg.get('embersId'):
        msg['embersId'] = msg['embers_id']
        del msg['embers_id']

    if is_us_tweet(msg):
        log.warn('Supressing US tagged tweet id=%s' % (msg.get('id_str') or msg.get('twitter', {}).get('id', 'UNKNOWN'),))
        return None

    msg = add_embers_ids(msg)

    return msg


def remove_new_line_char(msg):
    # convert to string and remove new line characters and convert back to dict
    try:
        msgstr = json.dumps(msg, encoding='utf=8', ensure_ascii=False)
        clean_msgstr = re.sub(ur'[\u0085\u2028]+', '', msgstr)
        clean_msgstr = clean_msgstr.replace("\\n", '')
        msg = json.loads(clean_msgstr, encoding='utf-8')
        return msg
    except Exception as e:
        log.exception('Failed to remove new line character(s).')
        return None

##
## convert language id in ingest feeds to standard 639-3 code.
##
LANGUAGE_ID = {
    "ar": "ara",
    "bg": "bul",
    "ca": "cat",
    "da": "dan",
    "de": "deu",
    "el": "ell",
    "en": "eng",
    "en-gb": "eng",
    "en-us": "eng",
    "es": "spa",
    "es-es": "spa",
    "es-mx": "spa",
    "et": "est",
    "eu": "eus",
    "fa": "fas",
    "fi": "fin",
    "fil": "fil",
    "fr": "fra",
    "gl": "glg",
    "he": "heb",
    "ht": "hat",
    "hu": "hun",
    "id": "ind",
    "is": "isl",
    "it": "ita",
    "ja": "jpn",
    "ka": "kat",
    "ko": "kor",
    "lt": "lit",
    "lv": "lav",
    "nl": "nld",
    "no": "nor",
    "pl": "pol",
    "pt": "por",
    "ro": "ron",
    "ru": "rus",
    "sk": "slk",
    "sl": "slv",
    "sv": "swe",
    "th": "tha",
    "tl": "tgl",
    "tr": "tur",
    "uk": "ukr",
    "vi": "vie",
    "zh": "zho",
    "zh-cn": "cmn",
    "zh-tw": "yue"
}


def normalize_language(obj, paths):
    '''
    Find language field(s) in an object and convert it to
    a UTC ISO 639-3 Code and write it to 'embersLang' field of the object.
    pathes - array of sequence of field names describing the full path to the language field.
    '''
    if obj.has_key('embersLang'):
        lang = obj.get('embersLang', None)
        if lang:
            return obj

    languages = []
    for path in paths:
        patharray = path.split('/')
        tmp = obj
        for p in patharray:
            if isinstance(tmp, dict):
                tmp = tmp.get(p, None)

        if tmp:
            log.debug("path=%s   tmp=%s" % (path, tmp))
            lang = LANGUAGE_ID.get(tmp.lower(), None)
            if lang:
                languages.append(lang)

    if len(languages) > 0:
        obj['embersLang'] = Counter(languages).most_common()[0][0]
    else:
        obj['embersLang'] = 'und'
        
    return obj
