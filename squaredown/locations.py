"""Locations class module for Squaredown.
"""
from aracnid_logger import Logger

from squaredown.connector import Connector

# initialize logging
logger = Logger(__name__).get_logger()


class Locations(Connector):
    """Contains the code to connect and pull locations from Square to MongoDB.

    Environment Variables:
        None.

    Attributes:
        collection: Square Orders collection in MongoDB.
        collection_name: Name of the Square Orders collection in MongoDB.
    """

    def __init__(self):
        """Initializes the Locations Connector.

        Establishes connections to Square and MongoDB.
        Sets up access to configuration properties.

        """
        self.collection_name = 'square_locations'
        logger.debug(f'collection_name: {self.collection_name}')
        super().__init__(config_name=self.collection_name)

        # initialize MongoDB collection
        self.collection = self.read_collection(self.collection_name)

    def pull(self):
        """Retrieves a set of Square Locations and saves them in MongoDB.

        Args:
            None

        Returns:
            None
        """
        logger.debug('pulling')

        result = self.api_locations.list_locations()

        locations = None
        if result.is_success():
            locations = result.body.get('locations')
        elif result.is_error():
            logger.error(result.errors)

        update_count = 0
        if locations:
            for location in locations:
                self.update_location(location)
                update_count += 1

        logger.debug(f'locations processed: {update_count}')

    def update_location(self, location):
        """Save the provided Square Location into MongoDB.

        Args:
            location: Square Location object

        Returns:
            The MongoDB representation of the Square Location object.
        """
        self.decode_location(location)

        # get the location properties
        location_id = location['id']

        # update the database
        self.mdb.square_locations.find_one_and_replace(
            filter={'_id': location_id},
            replacement=location,
            upsert=True
        )

        return location
