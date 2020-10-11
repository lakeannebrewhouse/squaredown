"""Class module that connects to both Square and MongoDB.
"""
from datetime import datetime
import os

from aracnid_logger import Logger
from aracnid_utils import timespan as ts

from squaredown.config import Config
from squaredown.i_mongodb import MongoDBInterface
from squaredown.i_square import SquareInterface

# initialize logging
logger = Logger(__name__).get_logger()

logger.debug('module installed')


class Connector(SquareInterface, MongoDBInterface):
    """Provides interfaces to Square and MongoDB to enable data exchange.

    This class can be used on its own or inherited, but it is more useful to
    create subclasses that inherit from the Connector class for specific data
    type. The attributes and instance methods help with this.

    Environment Variables:
        SQUAREDOWN_START_STR: The minimum start time for Connector operations.

    Attributes:
        config_name: Name of the configuration object in MongoDB.
        props: Configuration Properties object.
        start_min: Minimum start time to process objects
    """

    def __init__(self, config_name):
        """Initializes the interfaces and instance attributes.
        """
        SquareInterface.__init__(self)
        MongoDBInterface.__init__(self)

        self.config_name = config_name
        logger.debug(f'config_name: {self.config_name}')
        self.props = Config(self.config_name)

        self.set_start_min()

    def set_start_min(self):
        """Sets an attribute for the minimum start time.

        The minimum start time is determined by the environment.

        Args:
            None
        """
        start_str = os.environ.get('SQUAREDOWN_START_STR')
        self.start_min = datetime.fromisoformat(start_str).astimezone()

    def timespan(self, **kwargs):
        """Calculates the endpoints of a timespan.

        This instance method supplies the generic timespan function with more
        specific start times based on the last updated order or a preset
        minimum start time.

        Args:
            kwargs: Keyword arguments that specify the timespan.

        Returns:
            The start and end datetime objects that define the timespan.
        """
        if 'begin' not in kwargs and 'begin_str' not in kwargs:
            kwargs['begin'] = self.datetime_begin()

        return ts(**kwargs)

    def datetime_begin(self):
        """Provides the start time based on the last updated order.

        If no orders have been saved, this instance method defaults to the
        minimum start time preset from the environment.

        Returns:
            Datetime object that represents the start time of the timespan.
        """
        last_updated = self.props.last_updated

        if last_updated:
            return last_updated

        return self.start_min
