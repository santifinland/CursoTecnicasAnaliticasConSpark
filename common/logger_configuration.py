# -*- coding: utf-8 -*-

u"""
Técnicas analíticas con Spark y modelado predictivo
"""

# Stdlib imports
import logging
import logging.config
from os import environ, makedirs
from os.path import exists, join

# Project imports
from common.singleton import Singleton


# Disable unneeded logging stuff, although they are not logged, they are calculated
# Disable logging current thread
logging.logThreads = 0
# Disable logging PID
logging.logProcesses = 0
# Disable logging module, function or file line
logging._srcfile = None

CS_FORMAT = "time=%(asctime)s.%(msecs)03dZ | lvl=%(levelname)s | msg=%(message)s"
CS_CONSOLE_FORMAT = "%(asctime)s.%(msecs)03dZ | %(levelname)s | %(message)s"
LOGS_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'class': 'logging.Formatter',
            'format': CS_FORMAT,
            'datefmt': LOGS_DATE_FORMAT
        },
        'simple': {
            'class': 'logging.Formatter',
            'format': CS_CONSOLE_FORMAT,
            'datefmt': LOGS_DATE_FORMAT
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'simple',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': 'esic.log',
            'mode': 'w',
            'formatter': 'simple',
            'maxBytes': 10485760,
            'backupCount': 5
        },
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console', 'file']
    },
}


class LoggerManager(object):
    """Utility singleton class to configure loggers (only one for now):
    - one for the application traces

    This class should be initialized when launching the script. After that, just use the regular python logging system:

    To count on an application logger:

        >>> import logging
        >>> logger = logging.getLogger(__name__)

    """

    __metaclass__ = Singleton

    def __init__(self):
        """Logger initializer
        """
        # Create default directory for log files
        log_filename = LOG_CONFIG['handlers']['file']['filename']
        log_dir = environ['HOME'] + '/log/'
        if not exists(log_dir):
            makedirs(log_dir)
        LOG_CONFIG['handlers']['file']['filename'] = join(log_dir, log_filename)

        # Config logger
        logging.config.dictConfig(LOG_CONFIG)


# Get application logger
LoggerManager()
logger = logging.getLogger()
logger_spark = logging.getLogger('py4j')
logger_spark.setLevel(logging.INFO)
