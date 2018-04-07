#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

import collections
import os
import unicodedata
import time
import datetime
import json
import calendar
import copy
from .logging_conf import gen_logger
import atexit
import weakref
import hashlib
import pytz
import warnings

logger = gen_logger("utils")

TIME_FORMAT = "%a, %d %b %Y %H:%M:%S +0000"
TIME_ISOFORMAT = "%Y-%m-%dT%H:%M:%S.%f"
TIME_ISOFORMAT2 = "%Y-%m-%dT%H:%M:%S"

minjsondump = lambda o: json.dumps(o, skipkeys=True, indent=None,
                                   separators=(",", ":"))


def twittime_to_dt(t):
    return datetime.datetime(*(time.strptime(t, TIME_FORMAT)[0:6]),
                          tzinfo=pytz.UTC)


def isotime_to_dt(t):
    warnings.warn("This function returns a naive datetime"
                  " and should not be used", FutureWarning)
    try:
        d = datetime.datetime.strptime(t, TIME_ISOFORMAT)
        return d.replace(tzinfo=pytz.UTC)
    except:
        d = datetime.datetime.strptime(t, TIME_ISOFORMAT2)
        return d.replace(tzinfo=pytz.UTC)


def uuid_to_dt(u):
    t = (u.time - 0x01b21dd213814000L) / 1e7
    return datetime.datetime.fromtimestamp(t, pytz.UTC)


def now():
    return datetime.datetime.now(pytz.UTC)


def dt_to_ts(dt):
    """Convert a datetime to a timestamp

    :param dt: datetime object
    :returns: floating point timestamp in seconds/epoch

    """
    ts = calendar.timegm(dt.utctimetuple())
    ts += dt.microsecond / 1.e6
    return ts


def normalize_str(s,lower=True):
    if isinstance(s, str):
        s = s.decode("utf8")
    s = unicodedata.normalize("NFKD", s)
    if lower:
        return s.encode('ASCII', 'ignore').lower()
    else:
        return s.encode('ASCII', 'ignore')


def flatten_dict(d, prefix=''):
    o = {(prefix + k): v for k, v in d.iteritems() if not isinstance(v, dict)}
    for k, v in d.items():
        if isinstance(v, dict):
            i = flatten_dict(v, prefix=prefix + k + "_")
            o.update({k: v for k, v in i.items()})
    return o


def normalize_payload(payload):
    if payload['interaction']['type'] != "twitter":
        logger.error("Non twitter payload enecountered '%s'" %
                     minjsondump(payload))

    tweet = copy.deepcopy(payload)
    users = []
    users += [tweet['interaction']['author']]
    del tweet['interaction']['author']
    if 'retweet' in tweet['twitter']:
        tweet[u'is_retweet'] = 1
        tweet['twitter'].update(tweet['twitter']['retweet'])
        del tweet['twitter']['retweet']

        users += [tweet['twitter']['retweeted']['user']]
        tweet['twitter']['retweeted']['user'] = users[1]['screen_name']
        tweet['twitter']['retweeted']['uid'] = users[1]['id']

        tweet['twitter']['retweeted']['created_at'] = \
          twittime_to_dt(tweet['twitter']['retweeted']['created_at'])
    else:
        tweet[u'is_retweet'] = 0

    users[0].update(tweet['twitter']['user'])

    tweet['interaction']['created_at'] = \
            twittime_to_dt(tweet['interaction']['created_at'])

    if 'links' in tweet['twitter']:
        tweet['twitter']['links'] = u" ".join(tweet['twitter']['links'])
    if 'domains' in tweet['twitter']:
        tweet['twitter']['domains'] = u" ".join(tweet['twitter']['domains'])
    if 'mentions' in tweet['twitter']:
        tweet['twitter']['mentions'] = u" ".join(tweet['twitter']['mentions'])

    tweet['twitter']['user'] = users[0]['screen_name']
    tweet['twitter']['uid'] = users[0]['id']
    tweet = flatten_dict(tweet)
    del tweet['twitter_text']
    del tweet['twitter_created_at']
    del tweet['twitter_source']
    if 'twitter_geo_longitude' in tweet:
        del tweet['twitter_geo_longitude']
        del tweet['twitter_geo_latitude']
    if 'twitter_retweeted_geo_longitude' in tweet:
        del tweet['twitter_retweeted_geo_longitude']
        del tweet['twitter_retweeted_geo_latitude']

    for u in users:
        u['created_at'] = twittime_to_dt(u['created_at'])
        if 'geo_enabled' in u and u['geo_enabled']:
            u['geo_enabled'] = 1
        if 'verified' in u and u['verified']:
            u['geo_enabled'] = 1

    return tweet, users


def datetimeU(*args, **kwargs):
    kwargs['tzinfo'] = pytz.UTC
    return datetime.datetime(*args, **kwargs)


def filter_date(src, start_t, stop_t, field="date"):
    for data in src:
        tdate = isotime_to_dt(data[field])
        if tdate >= start_t and tdate < stop_t:
            yield data


def read_mjson(fname):
    with open(fname) as f:
        for l in f:
            yield json.loads(l)


class FlatFileSaver(object):
    """A class for handling saving blocks of text into files blocked by a
    period"""

    def __init__(self, dir, prefix, keyfunc=None):
        """@todo: to be defined

        :param dir: @todo
        :param prefix: @todo

        """
        self._dir = dir
        self._prefix = prefix
        self._keyfunc = lambda t: (t.strftime("%Y-%m-%d") +
                                   ("-%02d" % (t.hour / 12 * 12)))
        self._files = collections.OrderedDict()
        self._maxopen = 2
        atexit.register(FlatFileSaver.cleanup, weakref.ref(self))

    @staticmethod
    def cleanup(self):
        self = self()
        if self:
            for k, f in self._files.iteritems():
                f.close()

    def open(self, key):
        print self._dir, self._prefix + "." + key
        filename = os.path.join(self._dir, self._prefix + "." + key)
        return open(filename, "a")

    def writeline(self, time, line):
        k = self._keyfunc(time)
        f = None
        if k not in self._files:
            if len(self._files) >= self._maxopen:
                _, f = self._files.popitem(last=False)
                f.close()
            self._files[k] = self.open(k)
        f = self._files[k]
        f.write(line)
        f.write("\n")


def annotate_msg(msg):
    """Enriches a message per EMBERS policy

    :param msg:  message to be enriched
    :returns: enriched message

    """
    if 'data' in msg:
        data = msg.data['data']
    else:
        data = msg

    if 'embers_id' in data:
        data['embersId'] = data['embers_id']
        del data['embers_id']
    elif 'embersId' not in data:
        data['embersId'] = hashlib.sha1(str(data)).hexdigest()

    if 'date' not in data:
        try:
            created_interaction = data["interaction"]["created_at"]
            dt = twittime_to_dt(created_interaction)
            created_interaction_iso = dt.isoformat()
            data['date'] = created_interaction_iso
        except:
            data['date'] = "NULL"

    return data


def fileobj(strobj, mode="r"):
    """@todo: Docstring for fileobj

    :param strobj: either a string or a file object, if the former open a file
                   return if the latter pass it through
    :returns: a file object

    """
    if isinstance(strobj, basestring):
        return open(strobj, mode)
    else:
        assert(hasattr(strobj, 'read'))
        return strobj
