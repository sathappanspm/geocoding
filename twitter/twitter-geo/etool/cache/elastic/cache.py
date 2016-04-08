__author__ = 'mogren'
"""
Functions to insert messages into the Elasticsearch cache
"""

import general
import index_setup
import time
import math
import json
import sys
import boto
import os
import os.path
from dateutil import parser
from datetime import datetime, timedelta
import pytz
from etool import aws, logs, queue
from elasticsearch import helpers
from multiprocessing import Pool

log = logs.getLogger(__name__)


def cache_queue(queue_name):
    """
    Begins caching the messages from the queue_name provided
    :param queue_name: str
    :return:
    """
    index_name = general.get_index_name()

    if not general.get_es_connection().indices.exists_type(index=index_name, doc_type=queue_name):
            # Create mapping if the queue has not been stored in Elasticsearch yet
            index_setup.add_type(index_name=index_name, type_name=queue_name)

    attach_to_queue(index_name=index_name, queue_name=queue_name, type_name=queue_name)


def batch_messages(iterable_obj, es_index_name, es_type, batch_size=10, limit=None, wait_time=60):
    """
    Inserts the messages from iterable_obj of JSON objects into Elasticsearch in batches
    :param iterable_obj:
    :param es_index_name:
    :param es_type:
    :param batch_size: maximum amount of docs to send in one batch
    :param limit: maximum amount of docs from iterable_obj to store in Elasticsearch
    :param wait_time: maximum amount of time to wait in seconds before inserting a batch of documents into Elasticsearch
    :return:
    """
    count = 0
    message_batch = []
    pool = Pool(processes=4)

    timeout = int(round(time.time())) + wait_time

    for message in iterable_obj:
        if message:
            message_batch.append(message)
            count += 1

        # If we don't reach the batch size for a particular queue, perform the insert within the time_limit
        if len(message_batch) > 0 and (len(message_batch) >= batch_size or time.time() >= timeout):
            pool.apply_async(push, (message_batch, es_index_name, es_type))
            message_batch = []
            timeout = int(round(time.time())) + wait_time

        if limit and count >= limit:
            log.info('Inserted %s total message(s) into Elasticsearch' % count)
            return


def push(iterable_obj, es_index_name, es_type, chunk_size=100, elasticsearch=None):
    """
    Push the iterable_obj of JSON objects to Elasticsearch
    :param iterable_obj:
    :param es_index_name:
    :param es_type:
    :param chunk_size:
    :param elasticsearch:
    :return:
    """
    if elasticsearch is None:
        elasticsearch = general.get_es_connection()

    log.debug('Inserting into Elasticsearch')
    (successful_count, errors) = helpers.bulk(elasticsearch, iterable_obj, chunk_size=chunk_size, index=es_index_name,
                                              doc_type=es_type)
    log.debug('Inserted %s messages successfully' % successful_count)
    for error in errors:
        log.error(error)

    return successful_count, errors


def attach_to_s3(index_name, s3prefix, bucket, type_name, tmpcopy="tmp", chunk_size=100, startdate=None, enddate=None):
    """
    Attaches to S3 keys from a list generated based on an S3 prefix and inserts the messages into Elasticsearch
    :param index_name:
    :param type_name:
    :param limit:
    :return:
    """

    try:
        if startdate:
            startdate=parser.parse(startdate).replace(tzinfo=pytz.UTC)
    
        if enddate:
            enddate=parser.parse(enddate).replace(tzinfo=pytz.UTC)
    
        l = bucket.list(prefix=s3prefix)
        tot_cnt = 0
        tot_keys_in = 0
        tot_lines = 0
        for k in l:
            log.warning("Processing key %s" % k.name)
            if startdate or enddate:
                lmod = parser.parse(k.last_modified)
                if startdate and lmod < startdate:
                    log.warning("Skipping key %s: too old" % k.name)
                    continue
                if enddate and lmod > enddate:
                    log.warning("Skipping key %s: too new" % k.name)
                    continue
    
            tot_keys_in += 1
            try:
                k.get_contents_to_filename(tmpcopy)
            except Exception as e:
                log.error("Error getting contents from S3 key %s to %s: %s" % (k.name, tmpcopy, str(e)))
                continue
            if not os.path.isfile(tmpcopy):
                log.error("Local file %s is missing" % (tmpcopy))
                continue
    
            log.warning("Downloaded locally")
            k_cnt = 0
            with open(tmpcopy, 'r') as floc:
                lines = []
                for l in floc:
                    lines.append(json.loads(l.strip()))
    
                nl = len(lines)
                tot_lines += nl
                nch = int(math.ceil(float(nl)/chunk_size))
                log.warning("%d chunks of length %d" % (nch, chunk_size))
    
                start=0
                for ich in range(nch):
                    fin=min((ich+1)*chunk_size,nl)
                    log.warning("processing chunk %d: [%d:%d]" % (ich, start, fin))
                    try:
                         ch_count, errors = push(iterable_obj=lines[start:fin], es_index_name=index_name, es_type=type_name)
                         k_cnt += ch_count
                    except Exception as e:
                         log.error("Error processing chunk %d of %d: %s" % (ich, nch, str(e)))
                    start = fin
    
                log.warning("key %s: %d lines, %d good" % (k.name, nl, k_cnt))
                tot_cnt += k_cnt
            os.remove(tmpcopy)
        log.warning("Prefix %s: total ingested keys %d, total lines %d, total good %d" % (s3prefix, tot_keys_in, tot_lines, tot_cnt))
    except Exception as eTop:
        log.error("Top level exception: %s" % str(eTop))
    

def attach_to_queue(index_name, queue_name, type_name=None, limit=None):
    """
    Attaches to the queue_name provided and inserts the messages into Elasticsearch
    :param index_name:
    :param queue_name:
    :param limit:
    :return:
    """
    queue.init()
    log.debug('Attempting to attach to the queue %s' % queue_name)
    with queue.open(name=queue_name, mode='r') as message_queue:
        if limit:
            batch_messages(iterable_obj=message_queue, es_index_name=index_name, es_type=type_name, limit=limit)
        else:
            return push(iterable_obj=message_queue, es_index_name=index_name, es_type=type_name)


def cache_from_simple_db(domain_name, begin=None, end=None, limit=None, es_type_name=None):
        """
        Connects to the SimpleDB domain to retrieve the
        raw objects from the domain_name provided and store them in Elasticsearch.
        """
        if es_type_name is None:
            es_type_name = domain_name

        push(iterable_obj=aws.query_simple_db(domain_name=domain_name, begin=begin, end=end, limit=limit),
             es_index_name=general.get_index_name(), es_type=es_type_name)


def main():
    """
    Utility to cache messages from a queue into Elasticsearch
    -q | --queue   : Read from <queue> and write the messages to Elasticsearch. Settings are read from embers.conf
    --log_file     : Path to write the log file to
    --log_level    : Logging level
    """
    from etool import args
    global log

    arg_parser = args.get_parser()
    arg_parser.add_argument('-q', '--queue', help='Queue name to index into Elasticsearch')
    arg_parser.add_argument('-s', '--s3fromq', action='store_true', help='ingest from S3 prefix derived from queue name')
    arg_parser.add_argument('-p', '--prefix', help='Ingest from prefix')
    #arg_parser.add_argument('-t', '--typename', default='noqueue', help='Type for prefix ingest')
    arg_parser.add_argument('-t', '--typename', help='Type for prefix ingest')
    arg_parser.add_argument('-l', '--tmpcopy', default='/home/embers/data/tmpcopy',help='Name of local copy of S3 file (same for all S3 files)')
    arg_parser.add_argument('-c', '--chunk', type=int, default=100,help='Chunk size for S3 ingest')
    arg_parser.add_argument('-i', '--clustername', help='Clustername to determine index name')
    arg_parser.add_argument('-w', '--withbase', action="store_true", help="Add basename to prefix when looking for type.")
    arg_parser.add_argument('--startdate', help='start date in format like 2015-01-02')
    arg_parser.add_argument('--enddate', help='end date in format like 2015-01-02')
    arg = arg_parser.parse_args()

    #assert (arg.queue or (arg.prefix and arg.typename)), 'Either --queue (with optional --s3fromq/--typename) or --prefix with --typename must be provided'
    assert (arg.queue or arg.prefix ), 'Either --queue (with optional --s3fromq/--typename) or --prefix  must be provided'

    log = logs.getLogger(log_name=arg.log_file)
    logs.init(arg, l=arg.log_level, logfile=arg.log_file)

    index_name = general.get_index_name(arg.clustername)

    queue.init()

    if arg.prefix or (arg.queue and arg.s3fromq):
        if arg.prefix:
            prefix = arg.prefix
            # get queue name or its substitute for S3 objects from prefix
            if arg.typename:
                type_name = arg.typename
            else:
                type_name = queue.conf.get_prefixpair(prefix=prefix,includeS3=True,withBasename=arg.withbase)
                if not type_name:
                    log.error("Could not get type from prefix %s" % prefix)
                    return 1
                log.warning("type_name=%s from prefix=%s" % (type_name, prefix))
        else:
            type_name = arg.queue
            prefix, include = queue.conf.get_prefix_for_queue(type_name, withBasename=False)
            if not prefix:
                log.error("Could not get S3 prefix for queue %s" % type_name)
                return 1

        if not general.get_es_connection().indices.exists_type(index=index_name, doc_type=type_name):
            # Create mapping if the queue has not been stored in Elasticsearch yet
            index_setup.add_type(index_name=index_name, type_name=type_name)

        conn_s3 = boto.connect_s3(aws_access_key_id=arg.aws_key, aws_secret_access_key=arg.aws_secret)
        bucket = conn_s3.get_bucket(arg.bucket)	 # connect to S3, get bucket ptr for arg.bucket
        attach_to_s3(index_name, 
                     s3prefix=prefix, 
                     bucket=bucket, 
                     type_name=type_name, 
                     tmpcopy=arg.tmpcopy, 
                     chunk_size=arg.chunk,
                     startdate=arg.startdate,
                     enddate=arg.enddate)
    else:

        if arg.typename:
            type_name=arg.typename
        else:
            type_name=arg.queue

        if not general.get_es_connection().indices.exists_type(index=index_name, doc_type=type_name):
            # Create mapping if the queue has not been stored in Elasticsearch yet
            index_setup.add_type(index_name=index_name, type_name=type_name)

        attach_to_queue(index_name=index_name, queue_name=arg.queue, type_name=type_name)

if __name__ == '__main__':
    sys.exit(main())
