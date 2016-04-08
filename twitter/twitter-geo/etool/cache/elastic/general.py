__author__ = 'mogren'

from elasticsearch import Elasticsearch
import json
from os import environ
from etool import logs
import dateutil.parser as parser
import datetime

log = logs.getLogger(__name__)

# TODO retrieve values from a config file
# Used public DNS of Elastic IP since machines in AWS can't access public IP
es_host = 'ec2-107-22-214-53.compute-1.amazonaws.com'
es_port = 9200

elasticsearch = None
elasticsearch_index = None

AGG_TYPE_DATE_HISTOGRAM = 'date_histogram'
AGG_TYPE_TERMS = 'terms'


def get_es_connection():
    """
    Retrieves an Elasticsearch connection. Sets the global elasticsearch variable if it has not been set
    :return: Elasticsearch
    """
    global elasticsearch

    if elasticsearch is None:
        log.debug('Retrieving Elasticsearch connection to host %s on port %s' % (es_host, es_port))
        elasticsearch = Elasticsearch([{'host': es_host, 'port': es_port}])

    return elasticsearch


def get_index_name(cluster_name=None):
    """
    Returns the appropriate Elasticsearch index name for the current cluster
    :param cluster_name:
    :return: str Elasticsearch index name
    """
    global elasticsearch_index

    if elasticsearch_index is None:
        if cluster_name is None:
            cluster_name = get_cluster_name()

        if cluster_name is None or ('test' not in cluster_name and 'production' not in cluster_name):
            raise Exception("Invalid cluster_name provided: %s. Must contain either 'test' or 'production'" % cluster_name)

        # Create general index name
        if 'test' in cluster_name:
            elasticsearch_index = 'embers-test'
        elif 'production' in cluster_name:
            elasticsearch_index = 'embers-production'

    return elasticsearch_index


def get_cluster_name():
    """
    Retrieves the current cluster name from the CLUSTERNAME environment variable

    This should probably be in conf.py, but it creates dependency issues with ZMQ
    :return: str cluster name
    """
    return environ.get('CLUSTERNAME')


def query_es(query_string, search_type=None, es_index=None, es_type=None):
    """
    Queries the twitter-enriched ES index with the provided query_string
    :param query_string: str
    :param search_type: str optional
    :return: results
    """
    params = {}
    if search_type:
        params['search_type'] = search_type

    if es_index is None:
        es_index = get_index_name()

    if es_type and isinstance(es_type, str):
            es_type = [es_type]

    log.debug('Querying Elasticsearch index %s type %s: %s' % (es_index, es_type, query_string))
    results = get_es_connection().search(index=es_index, doc_type=es_type, body=query_string, params=params,
                                         timeout=50000)
    log.debug('Elasticsearch response: %s' % results)

    return results


def build_range_query(field, start=None, end=None):
    """
    Builds a range query for Elasticsearch. Returns None if state_time and end_time were not provided

    If end_time is not provided and start_time is, end_time will be the present time.

    :param field: str - Field in the Elasticsearch document to query
    :param start: ISO formatted date string
    :param end: ISO formatted date string
    :return: range_query dict | None
    """
    log.debug("Building a range query for field %s from '%s' to '%s'" % (field, start, end))
    range_query = None

    if field:
        if start:
            date = parser.parse(start)
            range_query = {'range': {field: {'gte': date.isoformat()}}}

        if end:
            date = parser.parse(end)

            if end == start:
                # Make sure end_date is end of the day
                date += datetime.timedelta(hours=23, minutes=59, seconds=59, milliseconds=59)

            if range_query:
                start_query = range_query['range'][field]
                start_query['lte'] = date.isoformat()
                range_query['range'][field] = start_query
            else:
                range_query = {'range': {field: {'lte': date.isoformat()}}}
    else:
        log.warn('Field not provided for range query. Returning None.')

    return range_query


def build_date_range_query(date_field, start_time=None, end_time=None):
    """
    Builds a range query for Elasticsearch. Returns None if state_time and end_time were not provided

    If end_time is not provided and start_time is, end_time will be the present time.

    :param date_field: str - Field in the Elasticsearch document to query
    :param start_time: ISO formatted date string
    :param end_time: ISO formatted date string
    :return: range_query dict | None
    """
    return build_range_query(date_field, start=start_time, end=end_time)


# TODO Name this better
def build_agg_query(agg_name, agg_type, agg_field, interval=None, order=None):
    agg_query = {agg_name: {agg_type: {'field': agg_field}}}

    if interval:
        # TODO Validate interval
        agg_query[agg_name][agg_type]['interval'] = interval

    if order:
        # TODO Validate order
        agg_query[agg_name][agg_type]['order'] = order

    return agg_query


def build_aggregation(agg_name, agg_type, agg_field=None, keywords=None, geo_box=None, start_time=None, end_time=None,
                      date_field=None, interval=None, include_regex=None, exclude_regex=None, order=None):
    if agg_type == AGG_TYPE_DATE_HISTOGRAM and interval is None:
        raise Exception('Interval must be set when building a date histogram')

    aggregation_query = {'aggs': None}
    if isinstance(agg_field, list):
        agg_queries = []
        for field in agg_field:
            index = agg_field.index(field)
            # TODO Make field plural
            name = field if index > 0 else agg_name
            agg_queries.append(build_agg_query(agg_name=name, agg_type=agg_type, agg_field=field, interval=interval,
                                               order=order))

        # Build nested aggregation from bottom up
        for agg_query in reversed(agg_queries):
            if aggregation_query['aggs'] is None:
                aggregation_query['aggs'] = agg_query
            else:
                prev_agg_query = aggregation_query['aggs']
                agg_name = agg_query.keys()[0]
                agg_query[agg_name]['aggs'] = prev_agg_query
                aggregation_query['aggs'] = agg_query
    else:
        aggregation_query['aggs'] = build_agg_query(agg_name=agg_name, agg_type=agg_type, agg_field=agg_field,
                                                    interval=interval, order=order)

    filter_query = build_query(keywords=keywords, geo_box=geo_box, start_time=start_time, end_time=end_time,
                               date_field=date_field)
    if filter_query:
        filter_query['aggs'] = aggregation_query['aggs']
        query = filter_query
    else:
        query = aggregation_query

    return query


def build_and_execute_aggregation(agg_name, agg_type, agg_field=None, search_type='count', keywords=None, geo_box=None,
                                  start_time=None, end_time=None, date_field=None, interval=None, include_regex=None,
                                  exclude_regex=None, order=None):
    query = build_aggregation(agg_name=agg_name, agg_type=agg_type, agg_field=agg_field, keywords=keywords,
                              geo_box=geo_box, start_time=start_time, end_time=end_time, date_field=date_field,
                              interval=interval, include_regex=include_regex, exclude_regex=exclude_regex, order=order)
    es_response = query_es(json.dumps(query), search_type=search_type)
    total = retrieve_total_hits(es_response=es_response)

    results = {}

    key_name = 'key'
    if agg_type == AGG_TYPE_DATE_HISTOGRAM:
        key_name += '_as_string'

    for hit in es_response['aggregations'][agg_name]['buckets']:
        if isinstance(agg_field, list):
            hit_results = {'total': hit['doc_count']}
            for subhit in hit[agg_field[1]]['buckets']:
                # TODO This is ugly
                if agg_field[1] not in hit_results:
                    # TODO This is ugly
                    hit_results[agg_field[1]] = {}

                # TODO This is ugly
                hit_results[agg_field[1]][subhit[key_name]] = subhit['doc_count']

            results[hit[key_name]] = hit_results
        else:
            results[hit[key_name]] = hit['doc_count']

    return {'total': total, agg_name: results}


def build_query(keywords=None, geo_box=None, start_time=None, end_time=None, date_field=None, max_results=None,
                fields_to_retrieve=None, default_operator='or', cache=False):
    """
    Builds a query for Elasticsearch with the parameters provided. Returns None if a query cannot be built

    If end_time is not provided and start_time is, end_time will be the present time.

    :param keywords: str, list, or dict - {'<field_name>': '<value>'} or {'<field_name>': [<list_of_values>}
    :param geo_box: dict {'lat': {'min': <value>, 'max': <value>}, 'lng':{'min': <value>, 'max': <value>}}
    :param start_time: ISO formatted date string
    :param end_time: ISO formatted date string
    :param max_results: int - Maximum amount of results to return
    :param fields_to_retrieve: list - Retrieves only these fields in the results if provided
    :return: dict object represent the Elasticsearch query to perform
    """
    query = None
    queries = []

    range_query = build_date_range_query(date_field=date_field, start_time=start_time, end_time=end_time)
    if range_query:
        queries.append(range_query)

    if keywords:
        if isinstance(keywords, str):
            queries.append({'query': {'simple_query_string': {'query': keywords, 'default_operator': default_operator}}})
        elif isinstance(keywords, list):
            keyword_list = ''
            for keyword in keywords:
                keyword_list += ' ' + keyword

            queries.append({'query': {'simple_query_string': {'query': keyword_list.strip(), 'default_operator': default_operator}}})
        elif isinstance(keywords, dict):
            for field in keywords:
                term_type = 'terms' if isinstance(keywords[field], list) else 'term'
                term_query = {term_type: {field: keywords[field], '_cache': cache}}
                queries.append(term_query)
        else:
            raise Exception('Invalid type for keywords: %s' % type(keywords).__name__)

    if geo_box:
            if 'lat_field' not in geo_box:
                raise Exception('"lat_field" is missing from geo_box')

            if 'lon_field' not in geo_box:
                raise Exception('"lon_field" is missing from geo_box')

            if 'lat' in geo_box and 'lng' in geo_box \
                    and 'min' in geo_box['lat'] and 'max' in geo_box['lat']\
                    and 'min' in geo_box['lng'] and 'max' in geo_box['lng']:
                # If the type of the field is geo-point: filter_query = {'geo_bounding_box': {'coordinates.coordinates': {'top_left': geo_box['ne'], 'bottom_right': geo_box['sw']}}}
                # Latitude bounds
                # This may need to be fixed if we are given coordinates at the axis of lon and lat
                queries.append(build_range_query(field=geo_box['lat_field'], start=geo_box['lat']['min'], end=geo_box['lat']['max']))
                # Longitude bounds
                queries.append(build_range_query(field=geo_box['lon_field'], start=geo_box['lng']['min'], end=geo_box['lng']['max']))
            else:
                log.error('Invalid geo_box provided: %s' % geo_box)
                raise Exception("Invalid geo_box provided. geobox = {'lat': {'min': <value>, 'max': <value>}, 'lng':{'min': <value>, 'max': <value>}}")

    if len(queries) > 0:
        # Use filter instead of query because we're not that fancy yet
        query = {'query': {'filtered': {'filter': {'and': {'filters': queries}}}}}

    if max_results:
        if query:
            query['size'] = max_results
        else:
            query = {'size': max_results}

    if fields_to_retrieve:
        if query:
            query['fields'] = fields_to_retrieve
        else:
            query = {'fields': fields_to_retrieve}

    log.debug('Successfully built Elasticsearch query: %s' % query)
    return query


def retrieve_data(keywords=None, geo_box=None, start_time=None, end_time=None, date_field=None, es_index=None,
                  es_type=None, max_results=10, fields_to_retrieve=None, cache=False):
    """
    Retrieves results from Elasticsearch containing the keywords provided in the provided time frame. If no parameters
    are provided, this function will return the 10 most recent documents in the index.
    :param keywords: dict - {'<field_name>': '<value>'} or {'<field_name>': [<list_of_values>}
    :param geo_box: dict {'lat': {'min': <value>, 'max': <value>}, 'lng':{'min': <value>, 'max': <value>}}
    :param start_time: ISO formatted date string
    :param end_time: ISO formatted date string
    :param es_index:
    :param es_type: str or list of specific types
    :param max_results: maximum amount of results to return
    :param fields_to_retrieve: str or list of fields to return in the results or dict if the field name should be
                transformed in the results returned { <field_to_retrieve>: <name_of_result>}
    :return: dict {'total': <total_results>, 'results': [list of results]}
    """
    query_string = None

    if isinstance(fields_to_retrieve, dict):
        field_names = fields_to_retrieve.keys()
    else:
        field_names = fields_to_retrieve

    query = build_query(keywords=keywords, geo_box=geo_box, start_time=start_time, end_time=end_time,
                        date_field=date_field, max_results=max_results, fields_to_retrieve=field_names, cache=cache)

    if query:
        query_string = json.dumps(query)

    es_response = query_es(query_string=query_string, es_index=es_index, es_type=es_type)

    total = retrieve_total_hits(es_response=es_response)
    results = []

    if 'hits' in es_response['hits']:
        for hit in es_response['hits']['hits']:
            if fields_to_retrieve:
                if 'fields' in hit:
                    field_results = {}

                    # In case fields_to_retrieve includes any ES fields
                    for field in hit:
                        if field in fields_to_retrieve:
                            key_name = field
                            if isinstance(fields_to_retrieve, dict) and fields_to_retrieve[field]:
                                # Translate the ES field name into the desired name for the results
                                key_name = fields_to_retrieve[field]

                            field_results[key_name] = hit[field]

                    for field in hit['fields']:
                        key_name = field
                        if isinstance(fields_to_retrieve, dict) and fields_to_retrieve[field]:
                            # Translate the ES field name into the desired name for the results
                            key_name = fields_to_retrieve[field]

                        if len(hit['fields'][field]) == 1:
                            # Return the value instead of a list if the field only has one value
                            value = hit['fields'][field][0]
                            field_results[key_name] = value
                        elif len(hit['fields'][field]) > 1:
                            value = hit['fields'][field]
                            field_results[key_name] = value

                    results.append(field_results)
                else:
                    log.debug("'fields' not included in hit: %s" % hit)
            else:
                results.append(hit['_source'])

    return {'total': total, 'results': results}


def retrieve_total_hits(es_response):
    """
    Parses the es_response to return the total matching documents contained in Elasticsearch
    :param es_response:
    :return: int total matching documents in Elasticsearch
    """
    total = 0

    if 'hits' in es_response and es_response['hits']:
        if 'total' in es_response['hits']:
            total = es_response['hits']['total']

    log.debug('Total retrieved: %s' % total)

    return total


def get_queue_names(index_name=None):
    """
    Retrieves all of the names of the queues currently held in cache by Elasticsearch
    :return: list
    """
    if index_name is None:
        index_name = get_index_name()

    es = get_es_connection()
    mappings = es.indices.get_mapping(index=index_name)

    if mappings and index_name in mappings:
        index_mapping = mappings[index_name]

        if index_mapping and 'mappings' in index_mapping:
            return index_mapping['mappings'].keys()
