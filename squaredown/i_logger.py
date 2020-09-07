"""Module to setup the Logger.

To find pesky log messages from imported modules, run this code from the main module.
loggerlist = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
print(loggerlist)

"""
import json
import logging
import logging.config
import os
import sys

# initialize module variables
NOTSET = logging.NOTSET
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL


def init_logger(config_dir=None):
    """Initializes the logger.

    If the configuration directory is not specified, the logging configuration
    file must be in the command directory, the same as the main calling
    application/function.

    Environment Variables:
        LOGGING_CONFIG_FILE: The name (and maybe relative path) of the
            logging configuration file.
        LOGGING_FORMATTER: Selected formatter specified in the config file.

    Args:
        config_dir: Directory containing the logging configuration file.
    """
    # get the filename of the config file
    logging_filename = os.environ.get('LOGGING_CONFIG_FILE')

    # get the directory of the logging config file
    logging_dir = config_dir
    if not logging_dir:
        command_dir = os.path.dirname(sys.argv[0])
        logging_dir = os.path.join(os.getcwd(), command_dir)

    # read the config file
    logging_path = os.path.join(logging_dir, logging_filename)
    with open(logging_path, 'rt') as file:
        logging_config = json.load(file)

    # update the formatter, based on the enrivonment
    formatter = os.environ.get('LOGGING_FORMATTER')
    logging_config['handlers']['console']['formatter'] = formatter

    # load the logger configuration
    logging.config.dictConfig(logging_config)

    # get the logger
    logger = logging.getLogger()

    return logger
