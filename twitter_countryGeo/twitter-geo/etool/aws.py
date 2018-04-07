#!/usr/bin/env python

import sys
import os
import boto
import datetime

def get_sdb_connection(args=None, key=None, secret=None):
    """Open a SDB connection based on the standard arguments from args."""
    k = os.environ.get('AWS_ACCESS_KEY_ID', None)
    s = os.environ.get('AWS_SECRET_ACCESS_KEY', None)

    if args and 'aws_key' in dir(args):
        k = args.aws_key

    if key:
        k = key

    if not k:
        raise Exception("No AWS key available, cannot continue.")

    if args and 'aws_secret' in dir(args):
        s = args.aws_secret

    if secret:
        s = secret

    if not s:
        raise Exception("No AWS secret available, cannot continue.")

    return boto.connect_sdb(k, s)


def init_domain(args=None, domain=None, suffix=None, key=None, secret=None):
    """
    Initializes a SDB domain
    :param args:
    :param domain:
    :param suffix:
    :param key:
    :param secret:
    :return:
    """
    d = None
    ds = None

    if args and 'sdbdomain' in dir(args):
        d = args.sdbdomain

    if domain:
        d = domain

    if not d:
        raise Exception("No domain specified, cannot continue.")

    if args and 'sdbsuffix' in dir(args):
        ds = args.sdbsuffix

    if suffix:
        ds = suffix

    if ds:
        d += ds

    conn = get_sdb_connection(args=args, key=key, secret=secret)

    if not conn.lookup(d):
        conn.create_domain(d)

    return conn.get_domain(d)


def get_domain(domain, key=None, secret=None):
    """
    Gets a handle to the SimpleDB domain if it
    exists. If it doesn't exists, we just give
    None rather than creating it.
    """
    return get_sdb_connection(key=key, secret=secret).lookup(domain)


def query_simple_db(domain_name, begin=None, end=None, limit=None):
    """
    Queries the Simple DB domain_name with the parameters provided and a returns an iterable set of results
    :param domain_name:
    :param begin:
    :param end:
    :param limit:
    :return:
    """
    where_clause = ''
    limit_clause = ''

    if begin is not None:
        # If no end range is given, assume that rows are to be retrieved up until now
        if not end:
            end = datetime.datetime.now().isoformat()
        where_clause = " where date >= '%s' AND date <= '%s'" % (begin, end)

    if limit:
        limit_clause = ' limit %s' % limit

    query = "select * from %s" % domain_name
    query += where_clause
    query += limit_clause

    # log.debug('Querying %s: %s' % (domain_name, query))
    return get_domain(domain=domain_name).select(query)


def test():
    import args
    p = args.get_parser()
    a = p.parse_args()
    d = init_domain(args=a)
    print d.name
    for i in d.select("select count(*) from %s" % (d.name,)):
        print i

    d = init_domain(domain='gsr_ingest', args=a)
    print d.name
    for i in d.select("select count(*) from %s" % (d.name,)):
        print i


if __name__ == "__main__":
    sys.exit(test())
