#!/usr/bin/env python

import os
import sys
import codecs
import json
from etool import args, logs, queue, iqueue
from geo2.country import GeoCountry

__processor__ = 'geo_code_stream.py'
log = logs.getLogger("geo_code_stream")  # '%s.log' % (__processor__))


def annotate(msg, geoc, filter_region=None):
    """
    Annotate message with geocountry info
    Params:
    msg - dict object
    geoc - GeoCountry object
    filter_region - region to be filtered
    """
    content = geoc.annotate(msg)
    content_region = content.get("embersGeoCodeCountry", {}).get("region", None)
    if content_region is None:
        return None

    if filter_region is not None and filter_region != content_region:
        return None

    return content


def main():
    '''
    Reads the  from the queue, retrieves the content
    from the source website and publishes the content to a new queue.
    '''
    ap = args.get_parser()
    ap.add_argument('--cat', action="store_true",
                    help='Read input from standard in and write to standard out.')
    ap.add_argument('--region', metavar='REGION', type=str, default=None,
                    help='Specify region to filter by')
    arg = ap.parse_args()
    logs.init(arg)
    filter_region = arg.region
    geoc = GeoCountry()
    try:
        if arg.cat:
            log.debug('Reading from stdin and writing to stdout.')
            ins = sys.stdin
            outs = codecs.getwriter('utf-8')(sys.stdout)
            for entry in ins:
                entry = entry.decode(encoding='utf-8')
                try:
                    tweet = json.loads(entry.strip())
                    tweet = annotate(tweet, geoc, filter_region)
                    if tweet is not None:
                        outs.write(json.dumps(tweet, ensure_ascii=False))
                        outs.write('\n')
                        outs.flush()
                except Exception:
                    log.exception('Failed to process message "%s".', entry)

        else:
            queue.init(arg)
            iqueue.init(arg)
            qname = "{}-geoCountry-{}".format(os.environ["CLUSTERNAME"], filter_region)
            with iqueue.open(arg.sub, 'r', qname=qname) as inq:
                with queue.open(arg.pub, 'w') as outq:  # , capture=True) as outq:
                    for tweet in inq:
                        try:
                            content = annotate(tweet, geoc, filter_region)
                            if content is not None:
                                outq.write(content)
                        except KeyboardInterrupt:
                            log.info("Got SIGINT, exiting.")
                            break
                        except Exception:
                            log.exception('Failed to process message "%s".', tweet)

        return 0

    except Exception as e:
        log.exception("Unknown error in main function-{0!s}.".format(e))
        return 1

if __name__ == '__main__':
    sys.exit(main())
