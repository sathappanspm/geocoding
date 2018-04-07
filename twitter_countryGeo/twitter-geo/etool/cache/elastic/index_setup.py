__author__ = 'mogren'
"""
Functions to setup the Elasticsearch indices
"""
import os.path
import sys
import json
import general
from etool import logs

log = logs.getLogger(__name__)

DEFAULT_SHARDS_NUM = 6
DEFAULT_REPLICAS_NUM = 1

CURRENT_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
MAPPING_FILE_PATH = os.path.join(os.path.join(CURRENT_DIRECTORY, 'conf'), 'mapping')  # ./conf/mapping/

# Default Elasticsearch type mapping if a custom mapping is not present
DEFAULT_MAPPING_FILENAME = os.path.join(MAPPING_FILE_PATH, 'default_mapping.json')


def add_type(index_name, type_name):
    """
    Loads in the type_name mapping file if it exists (scratch/etool/cache/elasticsearch/mapping/<type_name>.json) or
    uses the default mapping file if it doesn't and creates the Elasticsearch index/type
    :param index_name: Elasticsearch index name to create
    :param type_name: Elasticsearch type name to create
    :return:
    """
    # Retrieve and load custom mapping file if available
    file_name = os.path.join(MAPPING_FILE_PATH, type_name + '.json')

    if not os.path.isfile(file_name):
        file_name = DEFAULT_MAPPING_FILENAME

    mapping = json.load(open(file_name, 'r'))
    create_es_mapping(index_name=index_name, type_name=type_name, mapping=mapping)


def create_es_mapping(index_name, type_name=None, mapping=None, alias=None):
    """
    Function to verify and create the index_name if necessary and add the type_name with the mapping provided in
    Elasticsearch
    :param index_name: Elasticsearch index name to create
    :param type_name: Elasticsearch type name to create. Optional if you only want to create the index.
    :param mapping:
    :param alias: str optional alias name to apply to the index
    :return:
    """
    es = general.get_es_connection()

    if not es.indices.exists(index=index_name):
        # Make sure appropriate amount of shards are set up
        settings = {'settings': {'index': {'number_of_shards': DEFAULT_SHARDS_NUM,
                                           'number_of_replicas': DEFAULT_REPLICAS_NUM,
                                           'mapping.ignore_malformed': True}}}
        es.indices.create(index=index_name, body=settings)

    if alias:
        if es.indices.exists(index=alias):
            log.warn('Cannot create alias %s because an index with the same name exists')
        else:
            es.indices.put_alias(index=index_name, name=alias)

    if mapping:
        es.indices.put_mapping(index=index_name, doc_type=type_name, body=mapping)


def main():
    """
    Utility to set up a mapping for an EMBERS queue in Elasticsearch
    -q | --queue     : Queue name to set up the mapping for. Settings are read from embers.conf
    --log_file  : Path to write the log file to
    --log_level : Logging level
    """
    from etool import args
    global log

    arg_parser = args.get_parser()
    arg_parser.add_argument('-q', '--queue', help='Queue name to map into Elasticsearch')
    arg = arg_parser.parse_args()

    assert arg.queue, '--queue must be provided'

    log = logs.getLogger(log_name=arg.log_file)
    logs.init(arg, l=arg.log_level, logfile=arg.log_file)

    add_type(index_name=general.get_index_name(), type_name=arg.queue)

if __name__ == '__main__':
    sys.exit(main())
