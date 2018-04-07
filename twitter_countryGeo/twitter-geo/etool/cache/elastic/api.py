__author__ = 'mogren'
"""
General Elasticsearch queries for all feeds
"""

import general as es
from etool import logs
import sys

log = logs.getLogger(__name__)

twitter_date_field = 'date'
twitter_text_field = 'text'

twitter_queues = ['arabia-twitter-keyword', 'datasift-keyword', 'gnip-twitter-mena-keyword', 'twitter-keyword',
                  'twitter-mena-keyword']
rss_queues = ['arabia-news-keyword', 'rss-keyword', 'rss-mena-keyword']
warnings_queues = ['all-warnings']


def get_data(keywords=None, geo_box=None, start_time=None, end_time=None, sources=None, max_results=10):
    """
    Retrieves all data containing the keywords provided in the provided time frame. If no parameters are provided,
    this function will return the 10 most recent tweets in the index.

    If end_time is not provided and start_time is, end_time will be the present time.

    :param keywords: str, list of strings, or dict - {'<field_name>': '<value>'} or {'<field_name>': [<list_of_values>}
    :param geo_box: dict {'lat': {'min': <value>, 'max': <value>}, 'lng':{'min': <value>, 'max': <value>}}
    :param start_time: ISO formatted date string
    :param end_time: ISO formatted date string
    :param sources: list the queue names to retrieve
    :param max_results: the maximum amount of results to return
    :return: dict {'total': <value>, 'results': [{'timestamp': <value>, 'text':(value>,
                    'location': {'latitude': <value>, 'longitude': <value>}, 'image_url': <value>},
                    'noun_phrases': [<values>], 'lemmas': [<values], 'entities': [<values>]]}
    """
    log.debug("Retrieving data for keywords '%s'" % keywords)
    if geo_box:
        geo_box['lat_field'] = 'embersGeoCode.latitude'
        geo_box['lon_field'] = 'embersGeoCode.longitude'

    fields_to_retrieve = {twitter_date_field: 'timestamp', twitter_text_field: 'text', 'twitter.text': 'text',
                          'embersGeoCode.latitude': 'latitude', 'embersGeoCode.longitude': 'longitude',
                          'entities.media.media_url': 'image_url', 'twitter.entities.media.media_url': 'image_url',
                          'BasisEnrichment.tokens.lemma': 'lemmas', 'BasisEnrichment.nounPhrases.expr': 'noun_phrases',
                          'BasisEnrichment.entities.expr': 'entities', 'retweeted': 'retweeted', 'embersLang': 'lang',
                          'content': 'text', '_type': 'queue', 'content_image_url': 'image_url',
                          'coordinates': 'coordinates', 'location': 'location', 'FullText': 'text',
                          'DocURL': 'external_url', 'url': 'external_url'}

    queues_to_query = []
    if 'Twitter' in sources:
        queues_to_query += twitter_queues
        sources.remove('Twitter')

    if 'RSS' in sources:
        queues_to_query += rss_queues
        sources.remove('RSS')

    if 'Warnings' in sources:
        queues_to_query += warnings_queues
        sources.remove('Warnings')

    queues_to_query += sources

    if len([x for x in queues_to_query if x not in rss_queues]) == 0:
        # Limit number of results for RSS for performance reasons
        log.debug('Limiting number of results to 200 for RSS only query')
        max_results = 200

    if len(queues_to_query) > 0:
        results = es.retrieve_data(keywords=keywords, geo_box=geo_box, start_time=start_time, end_time=end_time,
                                   date_field=twitter_date_field, es_type=queues_to_query,
                                   max_results=max_results, fields_to_retrieve=fields_to_retrieve)

        for result in results['results']:
            if result["queue"] in twitter_queues:
                result['type'] = 'twitter'
            elif result["queue"] in rss_queues:
                result['type'] = 'rss'
            elif result["queue"] in warnings_queues:
                result['type'] = 'warning'

                if 'coordinates' in result:
                    coordinates_str = result['coordinates'].strip(' []')
                    coordinates = coordinates_str.split(',')
                    result['latitude'] = coordinates[0].strip()
                    result['longitude'] = coordinates[1].strip()

                    result['text'] = result['coordinates'] + ' ' + result['location']

        log.debug('Data received: %s' % results)
        return results


def get_queue_names():
    """
    Retrieves all of the names of the queues and their associated data source (i.e. Twitter, RSS, etc.) currently held
    in cache by Elasticsearch
    :return: dict {<type>: [<queue_name>], 'uncategorized': [<queue_name>]
    """
    queues = es.get_queue_names()
    result = {'RSS': [], 'Twitter': [], 'Warnings': [], 'uncategorized': []}

    for queue in queues:
        if queue in rss_queues:
            result['RSS'].append(queue)
        elif queue in twitter_queues:
            result['Twitter'].append(queue)
        elif queue in warnings_queues:
            result['Warnings'].append(queue)
        else:
            result['uncategorized'].append(queue)

    return result


def main():
    logs.init(l=logs.DEBUG)
    print(get_data(sources=['Warnings']))

if __name__ == '__main__':
    sys.exit(main())