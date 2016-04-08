__author__ = 'mogren'
"""
Utility to query the warnings that are stored in Elasticsearch
"""
import general as es
import sys
from etool import logs

log = logs.getLogger(__name__)


def query(embersId=None, date=None, eventType=None, model=None, location=None, eventDate=None, start_time=None,
          end_time=None, max_results=None):
    keywords = {}
    if embersId:
        keywords['_id'] = embersId

    # Probably could include this in **args, but I like named parameters better
    if date:
        keywords['date'] = date

    if eventType:
        keywords['eventType'] = eventType

    if model:
        keywords['model'] = model

    if location:
        keywords['location'] = location

    if eventDate:
        keywords['eventDate'] = eventDate

    return es.retrieve_data(keywords=keywords, start_time=start_time, end_time=end_time,
                               date_field='date', es_type=['warnings', 'all_warnings'], max_results=max_results)


def main():
    """
    Utility for warnings stored in Elasticsearch
    --log_file     : Path to write the log file to
    --log_level    : Logging level
    """
    from etool import args
    global log

    arg_parser = args.get_parser()
    arg = arg_parser.parse_args()

    log = logs.getLogger(log_name=arg.log_file)
    logs.init(arg, l=arg.log_level, logfile=arg.log_file)

    print(query(max_results=30))


if __name__ == '__main__':
    sys.exit(main())