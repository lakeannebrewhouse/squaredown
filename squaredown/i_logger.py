"""Module to setup the Logger.

To find pesky log messages from imported modules, run this code from the main module.
loggerlist = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
print(loggerlist)

"""
import json
import logging
import logging.config
import os

# initialize module variables
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR


def init_logger():
    """Initializes the logger.
    """
    # get the filename of the config file
    logging_filename = os.environ.get('LOGGING_CONFIG')

    # get the path of the logging config file
    logging_path = os.path.dirname(__file__)
    logging_fullpath = os.path.join(logging_path, logging_filename)

    # read the config file
    with open(logging_fullpath, 'rt') as file:
        logging_config = json.load(file)

    # update the formatter, based on the enrivonment
    formatter = os.environ.get('LOGGING_FORMATTER')
    logging_config['handlers']['console']['formatter'] = formatter

    # load the logger configuration
    logging.config.dictConfig(logging_config)

    # get the logger
    logger = logging.getLogger()

    return logger
