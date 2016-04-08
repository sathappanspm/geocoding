#!/usr/bin/env python

import sys
import re
import os
import os.path
import logging

# Valid logging levels
CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10

log_levels = {'critical': CRITICAL, 'fatal': FATAL, 'error': ERROR,
              'warning': WARNING, 'warn': WARN, 'info': INFO,
              'debug': DEBUG}


def get_log_file(logfile=None):
    if logfile:
        lgf = logfile
    elif os.environ.get('UPSTART_JOB'):
        lgf = os.environ['UPSTART_JOB']
    else:
        lgf = sys.argv[0]

    (d, p) = os.path.split(lgf)
    f = re.sub(r'(\.py)?$', '.log', p)

    path = os.path.join(os.getcwd(), 'logs')
    if not os.path.exists(path):
        path = os.path.join(os.path.realpath(d), 'logs')
        if not os.path.exists(path):
            path = os.path.realpath(d)

    return os.path.join(path, f)


def init(args=None, l=INFO, logfile=None):
    """
    Initialize a log in the logs directory using the name of the program.
    It first looks for logs in the current directory (typically ${HOME} in production).
    Then in the same directory as the program.
    The fallback is a file with the same name as the program and .log appended.
    """
    lf = get_log_file(logfile)

    format_str = ("%(asctime)s - %(name)s - %(levelname)s - %(process)d - %(thread)d"
                  " - %(funcName)s(%(lineno)d) - %(message)s")
    if isinstance(l, str):
        if l.lower() not in log_levels:
            # Default to 'info' if an invalid log_level is provided
            l = 'info'

        l = log_levels[l.lower()]

    if args and vars(args).get('verbose', False):
        l = logging.DEBUG

    if args and vars(args).get('log_stderr', False):
        logging.basicConfig(format=format_str, level=l)
    else:
        logging.basicConfig(filename=lf, format=format_str, level=l)


def getLogger(log_name=None):
    if not log_name:
        log_name = os.environ.get('UPSTART_JOB')
    if not log_name:
        log_name = sys.argv[0]

    return logging.getLogger(log_name)
