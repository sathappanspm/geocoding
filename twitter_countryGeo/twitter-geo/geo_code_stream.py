#!/usr/bin/env python

import sys
import json
from etool import args, logs, queue
from embers.geocode import Geo, GEO_REGION, PRIORITY_POLICY as LA_POLICY
from embers.geocode_mena import GeoMena, PRIORITY_POLICY as MENA_POLICY
from embers.utils import normalize_str

__processor__ = 'geo_code_stream.py'
log = logs.getLogger('%s.log' % (__processor__))

LOC_HEADERS = (u"geocode_version", u"city", u"country", u"admin1", u"admin2", u"admin3", u"pop",
               u"latitude", u"longitude", u"id", u"pad", u"source")


def decode(s, encoding='utf-8'):
    try:
        return s.decode(encoding=encoding)
    except:
        return s


def get_geoInfo(tweet, geo):
    geotuple = [decode(geo.__version__)] + [decode(l) for l in geo.geo_normalize(tweet)]
    return dict(zip(LOC_HEADERS, geotuple))


def isempty(s):
    """return if string is empty
    """
    if s is None or s == "" or s == "-":
        return True
    else:
        return False


def resolve_conflict(mena_geo, la_geo):
    if (not isempty(la_geo['city']) and
            normalize_str(la_geo['city']) == normalize_str(mena_geo['country'])):
        return mena_geo
    la_policy_num = LA_POLICY.index(la_geo['source'].split(",")[0])
    mena_policy_num = MENA_POLICY.index(mena_geo['source'].split(",")[0])
    la_policy_weight = round(float(la_policy_num) / float(len(LA_POLICY)), 1)
    mena_policy_weight = round(float(mena_policy_num) / float(len(MENA_POLICY)), 1)

    if la_policy_weight == mena_policy_weight:
        # Most the coarser level warning is the best or the warning with the higher population
        if isempty(la_geo['city']) and not isempty(mena_geo['city']):
            return la_geo
        elif isempty(mena_geo['city']) and not isempty(la_geo['city']):
            return mena_geo
        else:
            return la_geo
    else:
        if la_policy_weight < mena_policy_weight:
            return la_geo
        else:
            return mena_geo


def geo_annotate(tweet, geo_mena, geo_lac):
    if "embersGeoCodeCountry" in tweet:
        tweetregion = tweet["embersGeoCodeCountry"]["embersRegion"].lower()
        if tweetregion == "la":
            la_geo = get_geoInfo(tweet, geo_lac)
            tweet['embersGeoCode'] = la_geo
        elif tweetregion == "mena":
            mena_geo = get_geoInfo(tweet, geo_mena)
            tweet['embersGeoCode'] = mena_geo
        elif tweetregion is None:
            tweet['embersGeoCode'] = {
                "country": None,
                "admin1": None,
                "city": None
            }

        return tweet

    log.error("No embersGeoCodeCountry Found in tweet")
    # if embersGeoCodeCountry not in tweet, then run both geocoders
    # and resolve if there is any conflict
    mena_geo = get_geoInfo(tweet, geo_mena)
    la_geo = get_geoInfo(tweet, geo_lac)
    if mena_geo['country']:
        if not la_geo['country']:
            tweet['embersGeoCode'] = mena_geo
        else:
            tweet['embersGeoCode'] = resolve_conflict(mena_geo, la_geo)
            log.error("conflict, mena-%s, la-%s, resolved-%s" % (mena_geo['country'],
                                                                 la_geo['country'],
                                                                 tweet['embersGeoCode']['country']
                                                                 )
                      )
    else:
        tweet['embersGeoCode'] = la_geo

    return tweet


def main():
    '''
    Reads the  from the queue, retrieves the content
    from the source website and publishes the content to a new queue.
    '''
    ap = args.get_parser()
    ap.add_argument('--cat', action="store_true",
                    help='Read input from standard in and write to standard out.')
    arg = ap.parse_args()
    logs.init(arg)
    geo_mena = GeoMena()
    geo_lac = Geo(geo_region=GEO_REGION.lac)
    try:
        if arg.cat:
            log.debug('Reading from stdin and writing to stdout.')
            ins = sys.stdin
            outs = sys.stdout
            for entry in ins:
                entry = entry.decode(encoding='utf-8')
                try:
                    tweet = json.loads(entry.strip())
                    geo_annotate(tweet, geo_mena, geo_lac)
                    if tweet is not None:
                        outs.write(json.dumps(tweet, ensure_ascii=False).encode("utf-8"))
                        outs.write('\n')
                        outs.flush()
                except Exception:
                    log.exception('Failed to process message "%s".', (entry,))

        else:
            queue.init(arg)
            with queue.open(arg.sub, 'r') as inq:
                with queue.open(arg.pub, 'w', capture=True) as outq:
                    for tweet in inq:
                        try:
                            content = geo_annotate(tweet, geo_mena, geo_lac)
                            if content is not None:
                                outq.write(content)
                        except KeyboardInterrupt:
                            log.info("Got SIGINT, exiting.")
                            break
                        except Exception:
                            log.exception('Failed to process message "%s".', (tweet,))

        return 0

    except Exception as e:
        log.exception("Unknown error in main function-{}".format(str(e)))
        return 1

if __name__ == '__main__':
    sys.exit(main())
