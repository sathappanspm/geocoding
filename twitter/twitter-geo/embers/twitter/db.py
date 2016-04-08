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

import pycassa
from pycassa import system_manager as sm
from models import Tweets, Users, WordStats, WordRates

from cassa_config import TWITTER_KEYSPACE, SERVERS, TIMEOUT


class TwitterDB(object):
    """An interface to the Cassandra DB with a set of utility
    functions.
    """
    models = {'tweets': Tweets,
              'users': Users,
              'word_stats': WordStats,
              'word_rates': WordRates
             }

    def __init__(self):
        self.pool = pycassa.ConnectionPool(TWITTER_KEYSPACE,
                                           server_list=SERVERS,
                                           timeout=TIMEOUT)
        for name, m in self.__class__.models.items():
            setattr(self, name, m(self.pool))

    def make_query(self, name, query, sample=.1):
        """Generates a datasift query and registers is it with the database

        :param name: @todo
        :param query: @todo
        :param sample: @todo
        :returns: @todo

        """
        pass

    def save_tweet(self, tweet, tags=None):
        """Save tweet to the db, tag it with tags

        :param tweet: Normalized tweet payload
        :param tags: a list of strings representing tags

        """

        self.tweets.insert(tweet)
        _time = tweet['interaction_created_at']
        if tags:
            tweet['tags'] = ",".join(tags)

        self.word_stats.add("*", _time=_time)
        if tags:
            for tag in tags:
                if isinstance(tag, unicode):
                    tag = tag.encode("utf8")
                self.word_stats.add(tag, _time=_time)

    #def drop_tweet(self, uuid, tags=None):
    #    """ Delete a tweet from the db
    #    """
    #    self.tweets.remove(uuid)

    #    self.word_stats.add("*", -1, _time=_time)
    #    if tags:
    #        for tag in tags:
    #            if isinstance(tag, unicode):
    #                tag = tag.encode("utf8")
    #            self.word_stats.add(tag, -1, _time=_time)

    def save_users(self, users, time=None):
        """Save a set of users to the db
        """
        for user in users:
            self.users.insert(user, time=time)

    @classmethod
    def drop_keyspaces(cls):
        sysm = sm.SystemManager()
        sysm.drop_keyspace(TWITTER_KEYSPACE)

    @classmethod
    def create_keyspaces(cls):
        sysm = sm.SystemManager()
        if TWITTER_KEYSPACE not in sysm.list_keyspaces():
            sysm.create_keyspace(TWITTER_KEYSPACE, sm.SIMPLE_STRATEGY,
                                 {'replication_factor': '1'})
        for name, m in cls.models.items():
            m.create(sysm, TWITTER_KEYSPACE)

    @classmethod
    def alter_keyspaces(cls):
        sysm = sm.SystemManager()
        if TWITTER_KEYSPACE not in sysm.list_keyspaces():
            sysm.create_keyspace(TWITTER_KEYSPACE, sm.SIMPLE_STRATEGY,
                                 {'replication_factor': '1'})
        for name, m in cls.models.items():
            m.create_or_alter(sysm, TWITTER_KEYSPACE)


def main(args):
    import optparse
    parser = optparse.OptionParser()
    parser.usage = __doc__
    parser.add_option("-q", "--quiet",
                      action="store_false", dest="verbose", default=True,
                      help="don't print status messages to stdout")
    parser.add_option("", "--create",
                      action="store_true", dest="create", default=False,
                      help="don't print status messages to stdout")
    parser.add_option("", "--alter",
                      action="store_true", dest="alter", default=False,
                      help="don't print status messages to stdout")
    parser.add_option("", "--delete",
                      action="store_true", dest="delete", default=False,
                      help="don't print status messages to stdout")
    (options, args) = parser.parse_args()
    if len(args) < 0:
        parser.error("Not enough arguments given")

    if options.create:
        TwitterDB.create_keyspaces()
    elif options.delete:
        TwitterDB.drop_keyspaces()
    elif options.alter:
        TwitterDB.alter_keyspaces()
    else:
        tc = TwitterDB()  # NOQA
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main(sys.argv))
