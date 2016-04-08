__author__ = 'mogren'
"""
General Elasticsearch queries for Twitter feeds
"""

import sys
import general as es
import re
from etool import logs

log = logs.getLogger(__name__)

twitter_date_field = 'date'
twitter_text_field = 'text'


def get_tweets(keywords=None, geo_box=None, start_time=None, end_time=None, max_results=10):
    """
    Retrieves all tweets containing the keywords provided in the provided time frame. If no parameters are provided,
    this function will return the 10 most recent tweets in the index.

    If end_time is not provided and start_time is, end_time will be the present time.

    :param keywords: str, list of strings, or dict - {'<field_name>': '<value>'} or {'<field_name>': [<list_of_values>}
    :param geo_box: dict {'lat': {'min': <value>, 'max': <value>}, 'lng':{'min': <value>, 'max': <value>}}
    :param start_time: ISO formatted date string
    :param end_time: ISO formatted date string
    :param max_results: the maximum amount of results to return
    :return: dict {'total': <value>, 'results': [{'timestamp': <value>, 'text':(value>,
                    'location': {'latitude': <value>, 'longitude': <value>}, 'image_url': <value>},
                    'noun_phrases': [<values>], 'lemmas': [<values], 'entities': [<values>]]}
    """
    log.debug("Retrieving tweets for keywords '%s'" % keywords)
    if geo_box:
        geo_box['lat_field'] = 'embersGeoCode.latitude'
        geo_box['lon_field'] = 'embersGeoCode.longitude'

    fields_to_retrieve = {twitter_date_field: 'timestamp', twitter_text_field: 'text', 'twitter.text': 'text',
                          'embersGeoCode.latitude': 'latitude', 'embersGeoCode.longitude': 'longitude',
                          'entities.media.media_url': 'image_url', 'twitter.entities.media.media_url': 'image_url',
                          'BasisEnrichment.tokens.lemma': 'lemmas', 'BasisEnrichment.nounPhrases.expr': 'noun_phrases',
                          'BasisEnrichment.entities.expr': 'entities', 'retweeted': 'retweeted', 'embersLang': 'lang'}

    # Only twitter-* types will have the 'text' field
    results = es.retrieve_data(keywords=keywords, geo_box=geo_box, start_time=start_time, end_time=end_time,
                               date_field=twitter_date_field,
                               es_type=['datasift-keyword', 'twitter-keyword', 'twitter-mena-keyword'],
                               max_results=max_results, fields_to_retrieve=fields_to_retrieve)

    log.debug('Tweets received: %s' % results)
    return results


def aggregate_tweets_over_time(interval='hour', keywords=None, geo_box=None, start_time=None, end_time=None,
                               include_regex=None, exclude_regex=None):
    """
    Provides an aggregate count of tweets matching the parameters provided over the interval provided. If no parameters
    are provided, this function will return an aggregate count of all tweets in the index by hour.

    If end_time is not provided and start_time is, end_time will be the present time.

    :param interval: str
    :param keywords: dict - {'<field_name>': '<value>'} or {'<field_name>': [<list_of_values>}
    :param geo_box: dict {'lat': {'min': <value>, 'max': <value>}, 'lng':{'min': <value>, 'max': <value>}}
    :param start_time: ISO formatted date string
    :param end_time: ISO formatted date string
    :return: dict {'total': <total_results>, 'intervals': {<interval_timestamp>: <count_of_matching_tweets>}}
    """
    # TODO Default range to past month, change if start_time, end_time are provided
    if geo_box:
        geo_box['lat_field'] = 'embersGeoCode.latitude'
        geo_box['lon_field'] = 'embersGeoCode.longitude'

    return es.build_and_execute_aggregation(agg_name='tweets_over_interval', agg_type=es.AGG_TYPE_DATE_HISTOGRAM,
                                            agg_field=twitter_date_field, interval=interval)


def aggregate_tweets_by_country(country=None, lemmas=None, entities=None, start_time=None, end_time=None,
                                include_regex=None, exclude_regex=None):
    """
    Aggregates tweets by the parameters provided.

    !!!Inclusion of 'entities', time range, or regular expressions has not been developed yet
    :param country:
    :param lemmas:
    :param entities:
    :param start_time:
    :param end_time:
    :param include_regex:
    :param exclude_regex:
    :return:
    """
    keywords = None
    fields = 'embersGeoCode.country'

    if lemmas:
        keywords = {'lemma': lemmas}
        fields = [fields, 'lemma']

    if entities:
        if keywords:
            keywords['entity'] = entities
        else:
            keywords = {'entity': entities}

    if country:
        if keywords:
            keywords['embersGeoCode.country'] = country
        else:
            keywords = {'embersGeoCode.country': country}

    # TODO Adjust response to make 'lemma' and 'entity' plural
    # TODO include entity
    return es.build_and_execute_aggregation(agg_name='countries', agg_type=es.AGG_TYPE_TERMS, agg_field=fields,
                                            keywords=keywords, order={'_term': 'asc'})


def get_abbreviated_tweets_for_dqe(country=None, queue_name=None, start_date=None, end_date=None):
    """
    Returns ALL results (can be a very large set) for the parameters provided in the format:
        [[country, admin1, city, [lat, long], population], [lemmas], [hashtags], [user mentions], [urls], text, embersId]]

    If end_date is not provided and start_date is, end_date will be the present time.

    ***This is meant specifically for the Dynamic Query Expansion model***

    ***This will return a very large set of data***

    ***If country is not provided, the results will not be filtered or ordered in any kind of way. No results are
        guaranteed as the data may be so large it could crash the system***

    :param country: str
    :param queue_name: str or list of queues to query
    :param start_date: ISO formatted date string
    :param end_date: ISO formatted date string
    :return: [[country, admin1, city, [lat, long], population], [lemmas], [hashtags], [user mentions], [urls], text, embersId]]
    """
    keywords = None

    TARGET_POS = '''\A(NOUN).*|\A(PAP).*|\A(VERB).*|\A(ADJ).*|
    \A(ADVADV)\Z|\A(ADVINT)\Z|\A(PROP)\Z|\A(NPL)\Z|\A(VPP)\Z|
    \A(GER)\Z|\A(VPRES)\Z|\A(NSG)\Z|\A(VPAP)\Z|\A(PARTPRES)\Z|\A(PARTPAST)\Z|
    \A(VI)\Z|\A(NPROP)\Z|\A(PROP)\Z|\A(INF)\Z|\A(DEM).*|\A(DET).*|\A(POSS).*'''
    tar_pos = re.compile(TARGET_POS)

    if country:
        if keywords:
            keywords['embersGeoCode.country'] = country.title()
        else:
            keywords = {'embersGeoCode.country': country.title()}

    # Make sure all of documents are retrieved at once by setting the size to the maximum amount of documents
    size = es.retrieve_data(keywords=keywords, start_time=start_date, end_time=end_date, date_field=twitter_date_field,
                            es_type=queue_name, cache=True)['total']

    fields_to_retrieve = ['embersGeoCode.country', 'embersGeoCode.admin1', 'embersGeoCode.city',
                          'embersGeoCode.latitude', 'embersGeoCode.longitude', 'embersGeoCode.pop',
                          'BasisEnrichment.tokens.lemma', 'BasisEnrichment.tokens.POS', 'BasisEnrichment.entities.expr',
                          'BasisEnrichment.entities.neType', 'text', 'embersId']

    fields_to_report = [x for x in fields_to_retrieve
                            if x not in ['BasisEnrichment.tokens.POS', 'BasisEnrichment.entities.neType']]

    es_results = es.retrieve_data(keywords=keywords, start_time=start_date, end_time=end_date,
                                  date_field=twitter_date_field, fields_to_retrieve=fields_to_retrieve,
                                  es_type=queue_name, max_results=size, cache=True)

    results = []
    if 'results' in es_results:
        for result in es_results['results']:
            country = None
            admin1 = None
            city = None
            population = None
            lat_lon = []
            lemmas = []
            hashtags = []
            usernames = []
            urls = []
            embersId = []

            for field in fields_to_report:
                if field in result:
                    if 'embersGeoCode' in field:
                            if 'latitude' in field or 'longitude' in field:
                                lat_lon.append(result[field])
                            elif 'country' in field:
                                country = result[field].lower()
                            elif 'admin1' in field:
                                admin1 = result[field]
                            elif 'city' in field:
                                city = result[field]
                            elif 'pop' in field:
                                population = result[field]
                    elif 'BasisEnrichment.tokens.lemma' in field:
                        lemmas = [result['BasisEnrichment.tokens.lemma'][i] \
                                      for i in range(0, len(result['BasisEnrichment.tokens.POS'])) \
                                      if tar_pos.findall(result['BasisEnrichment.tokens.POS'][i])]

                    elif 'BasisEnrichment.entities.expr' in field:
                        hashtags = [result['BasisEnrichment.entities.expr'][i] \
                                      for i in range(0, len(result['BasisEnrichment.entities.neType'])) \
                                      if "TWITTER:HASHTAG" == result['BasisEnrichment.entities.neType'][i]]

                        usernames = [result['BasisEnrichment.entities.expr'][i] \
                                      for i in range(0, len(result['BasisEnrichment.entities.neType'])) \
                                      if "TWITTER:USERNAME" == result['BasisEnrichment.entities.neType'][i]]

                        urls = [result['BasisEnrichment.entities.expr'][i] \
                                      for i in range(0, len(result['BasisEnrichment.entities.neType'])) \
                                      if "IDENTIFIER:URL" == result['BasisEnrichment.entities.neType'][i]]
                    # All values are returned as lists even if they contain a single value
                    elif 'embersId' in field:
                        embersId = result['embersId']

            geocode_values = [country, admin1, city, lat_lon, population]
            results.append([geocode_values, lemmas, hashtags, usernames, urls, embersId])

    return results


def main():
    logs.init(l=logs.DEBUG)
    # TODO translate non-English characters
    # print(get_tweets(keywords='esta'))
    # print(aggregate_tweets_over_time())
    # print(aggregate_tweets_by_country())
    # print(aggregate_tweets_by_country(lemmas='bill'))
    # print(aggregate_tweets_by_country(country='brazil', lemmas='bill'))
    # print(aggregate_tweets_by_country(country='brazil', lemmas='bill', entities=''))

    print(get_abbreviated_tweets_for_dqe('brazil', 'datasift-keyword', start_date='2015-03-27', end_date='2015-03-27'))

if __name__ == '__main__':
    sys.exit(main())
