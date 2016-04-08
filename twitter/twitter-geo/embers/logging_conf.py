# -*- coding: UTF-8 -*-
# vim: ts=4 sts=4 sw=4 tw=79 sta et
"""%prog [options]
Python source code - @todo
"""

__author__ = 'Patrick Butler'
__email__ = 'pbutler@killertux.org'

import sys
import os
try:
    import yaml
except ImportError:
    print >>sys.stderr, ("YAML failed to import, reverting to default logging "
                         "config")

import logging
import logging.config

logging_conf = None

default_conf = {
    'version': 1,
    'formatters': {
        'simple': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
            'level': 'INFO',
            'stream': 'ext://sys.stdout'
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['console'],
    },
    'loggers': {
        'sub_cassa': {
            'level': 'DEBUG',
            'handlers': ['console'],
            'propagate': False,
        },
        'sub_flat': {
            'level': 'DEBUG',
            'handlers': ['console'],
            'propagate': False,
        },
        'querypubber': {
            'level': 'DEBUG',
            'handlers': ['console'],
            'propagate': False,
        },
    }
}


class LoggerAutoInit(logging.getLoggerClass()):
    def handle(self, record):
        if len(logging.root.handlers) == 0:
            if init_logger():
                self.disabled = 0
        return super(LoggerAutoInit, self).handle(record)

logging.setLoggerClass(LoggerAutoInit)


def gen_logger(name=None, options=None):
    logger = logging.getLogger(name)
    return logger


def init_logger(options=None):
    global logging_conf
    conf_fname = os.path.join('etc', 'embers', 'logging.conf')
    if not logging_conf:
        if os.path.exists(conf_fname):
            logging_conf = yaml.load(open(conf_fname))
        else:
            logging_conf = default_conf

        if options and options.logfile:
            logging_conf['handlers']['file']['filename'] = options.logfile
        logging.config.dictConfig(logging_conf)
        return True
    return False
