__author__ = 'mogren'
"""
General caching service
"""

from etool import conf
from os import environ
import sys
from etool import logs
import multiprocessing
from etool.cache.elastic.cache import cache_queue

log = logs.getLogger(__name__)


def main():
    """
    Utility to cache messages from all queues from the --hostname provided with 'cache: true' option set in embers.conf
    --hostname  : Cache all active queues on this host
    --log_file  : Path to write the log file to
    --log_level : Logging level
    """
    from etool import args
    global log

    arg_parser = args.get_parser()
    arg_parser.add_argument('--hostname', metavar='HOSTNAME', type=str, default=environ.get('HOSTNAME', None),
                            help="The hostname of the machine whose services' data you wish to cache")
    arg = arg_parser.parse_args()

    log = logs.getLogger(log_name=arg.log_file)
    logs.init(arg, l=arg.log_level, logfile=arg.log_file)
    conf.init(arg)

    assert arg.hostname, '--hostname must be provided'
    queues = conf.get_all_cached_queues(hostname=arg.hostname)
    pool = []

    for queue in queues:
        log.info('Spawning cache process for %s' % queue)
        p = multiprocessing.Process(name=queue, target=cache_queue, args=(queue,))
        p.start()
        pool.append(p)

    try:
        for process in pool:
            process.join()
            log.warn('%s caching has stopped' % process.name)
    except KeyboardInterrupt:
        log.warn('Keyboard interrupt in main')


if __name__ == '__main__':
    sys.exit(main())
