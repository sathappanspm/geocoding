#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo

This implements the code to store and save data about tweets

IMPORTANT NOTE: All times are in UTC.  They must be either naive and represent
UTC or contain valid tzinfo.  Eventually this shall not only naive datetimes
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'


from columnfamilies import CounterColumnFamily, StatColumnFamily
from columnfamilies import WideTimeColumnFamily, ColumnFamily
import datetime
from ..utils import dt_to_ts

import pycassa
from pycassa.util import convert_time_to_uuid


class Tweets(WideTimeColumnFamily):
    """Record a set of numerical stats"""
    name = "tweets"
    super = True
    start = datetime.datetime(2010, 1, 1)
    interval = 300
    default_type = pycassa.UTF8_TYPE
    column_name_type = pycassa.types.TimeUUIDType()
    subcolumn_name_type = pycassa.UTF8_TYPE
    columns = {
        'twitter_uid': pycassa.LONG_TYPE,
        'interaction_created_at': pycassa.DATE_TYPE,
        'followers_count': pycassa.INT_TYPE,
        'salience_content_sentiment': pycassa.INT_TYPE,
        'statuses_count': pycassa.INT_TYPE,
        'utc_offset_count': pycassa.INT_TYPE,
        'twitter_retweeted_created_at': pycassa.DATE_TYPE,
        'twitter_count': pycassa.INT_TYPE,
        'twitter_retweeted_uid': pycassa.LONG_TYPE,
        'klout_score': pycassa.INT_TYPE,
        'language_confidence': pycassa.INT_TYPE,
        'is_retweet': pycassa.INT_TYPE,
        'interaction_geo_longitude': pycassa.FLOAT_TYPE,
        'interaction_geo_latitude': pycassa.FLOAT_TYPE,
        'twitter_retweeted_geo_longitude': pycassa.FLOAT_TYPE,
        'twitter_retweeted_geo_latitude': pycassa.FLOAT_TYPE,
    }

    def row_key(self, _time):
        _time = int(dt_to_ts(_time) - self.start_ts)
        _time //= self.interval
        return str(int(_time))

    def insert(self, tweet, batch=None):
        time = tweet['interaction_created_at']
        return super(Tweets, self).insert(time, tweet, batch)


class Users(ColumnFamily):
    """Record users profiles over time"""
    name = "users"
    super = True
    key_type = pycassa.LONG_TYPE
    column_name_type = pycassa.types.TimeUUIDType(reversed=True)
    subcolumn_name_type = pycassa.UTF8_TYPE
    default_type = pycassa.UTF8_TYPE
    columns = {
        'id': pycassa.LONG_TYPE,
        'created_at': pycassa.DATE_TYPE,
        'followers_count': pycassa.INT_TYPE,
        'friends_count': pycassa.INT_TYPE,
        'statuses_count': pycassa.INT_TYPE,
        'listed_count': pycassa.INT_TYPE,
        'utc_offset': pycassa.INT_TYPE,
        'geo_enabled': pycassa.INT_TYPE,
        'verified': pycassa.INT_TYPE,
    }

    def insert(self, user, time, batch=None):
        id = user['id']
        ckey = convert_time_to_uuid(time, randomize=True)
        #name = user['screen_name'].lower()

        super(Users, self).insert(id, {ckey: user}, batch)
        #super(Users, self).insert("lookup|" + name, {"0": {"id": id}}, batch)

    def lookup_id(self, name):
        name = name.lower()
        ret = super(Users, self).get("lookup|" + name,
                                     super_column="0", column="id")
        if ret:
            return ret[0][0]['id']
        else:
            return None


class WordStats(CounterColumnFamily):
    name = "word_stats"
    start = datetime.datetime(2010, 1, 1)
    interval = 30 * 24 * 60 * 60  # roughly a month
    sub_interval = "m"


class WordRates(StatColumnFamily):
    name = "word_rates"
    start = datetime.datetime(2010, 1, 1)
    interval = 30 * 24 * 60 * 60  # roughly a month
    sub_interval = "m"
