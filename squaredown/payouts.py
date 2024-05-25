"""Payouts class module for Squaredown.
"""
# pylint: disable=no-member

import os

from aracnid_logger import Logger
from tqdm import tqdm

from squaredown.connector import Connector

# initialize logging
logger = Logger(__name__).get_logger()


class Payouts(Connector):
    """Contains the code to connect and pull payouts from Square to MongoDB.

    Environment Variables:
        SQUARE_LOCATIONS: List of Square Locations to process.

    Attributes:
        collection: Square Payouts collection in MongoDB.
        collection_name: Name of the Square Payouts collection in MongoDB.
        location_ids: Square location identifiers.
    """

    def __init__(self):
        """Initializes the Payouts Connector.

        Establishes connections to Square and MongoDB.
        Sets up access to configuration properties.

        """
        self.collection_name = 'square_payouts'
        logger.debug('collection_name: %s', self.collection_name)
        super().__init__(config_name=self.collection_name)

        # initialize MongoDB collection
        self.collection = self.mdb.read_collection(self.collection_name)
        self.location_ids = os.environ.get('SQUARE_LOCATIONS').split(',')

        # initialize payout entries
        self.payout_entries = PayoutEntries()

    def pull(self, **kwargs):
        """Retrieves/processes a set of Square Payouts and saves them in MongoDB.

        There is no duplicate checking because the query doesn't return objects
        based on update time, only start time.

        Args:
            **kwargs: keyword arguments that specify the timespan to retrieve

        Returns:
            None
        """
        # read the payouts
        payouts = self.read(**kwargs)

        # end if no payouts
        if not payouts or len(payouts) == 0:
            logger.info('payouts processed: 0')
            return

        update_count = 0
        for payout in tqdm(payouts, desc='payouts'):
            # update payout
            self.update_payout(payout)
            update_count += 1

            # pull the payout entries
            self.payout_entries.pull(payout['id'])

            # debug, only process one payout
            # break

        logger.info('payouts processed: %s', update_count)

    def read(self, **kwargs):
        """Returns a set of Square Payouts.

        Args:
            **kwargs: keyword arguments that specify the timespan to retrieve

        Returns:
            List of Square Payouts.
        """
        payouts = []
        start, end = self.timespan(collection='square_payouts', **kwargs)
        logger.debug('timespan: %s, %s', start, end)

        result = self.api_payouts.list_payouts(
            begin_time=start.isoformat(),
            end_time=end.isoformat()
        )

        if result.is_success():
            while result.body != {}:
                payouts.extend(result.body['payouts'])

                if result.cursor:
                    result = self.api_payouts.list_payouts(
                        begin_time=start.isoformat(),
                        end_time=end.isoformat()
                    )
                else:
                    break

        elif result.is_error():
            for error in result.errors:
                logger.error(
                    'error reading payout entries: %s, %s\n%s',
                    error['category'], error['code'], error['detail']
                )

        return payouts

    def update_payout(self, payout):
        """Save the provided Square Object into MongoDB.

        Args:
            payout: Square Payout object

        Returns:
            The MongoDB representation of the Square Payout object.
        """
        self.decode_payout(payout)

        # get payout properties
        payout_id = payout['_id'] = payout['id']
        updated_at = payout.get('updated_at')

        # log the update
        logger.info('update_payout %s: %s', payout_id, updated_at.isoformat()[0:16])

        # apply payout customizations
        self.apply_payout_customizations(payout)

        # update the database
        self.collection.find_one_and_replace(
            filter={'_id': payout_id},
            replacement=payout,
            upsert=True
        )

        return payout

    def apply_payout_customizations(self, payout: dict) -> None:
        """Apply customizations to the Square Payout object.

        This method should be overridden to apply app-specific customizations.
        The Square Payout object is modified directly.

        Args:
            payout (dict): Square Payout object
        """
        # pylint: disable=unused-argument

        # logger.debug('Applying default customizations: %s', self.collection_name)

        return

class PayoutEntries(Connector):
    """Contains the code to connect and pull payout entries from Square to MongoDB.

    Environment Variables:
        SQUARE_LOCATIONS: List of Square Locations to process.

    Attributes:
        collection: Square Payout Entries collection in MongoDB.
        collection_name: Name of the Square Payout Entries collection in MongoDB.
        location_ids: Square location identifiers.
    """

    def __init__(self):
        """Initializes the PayoutEntries Connector.

        Establishes connections to Square and MongoDB.
        Sets up access to configuration properties.

        """
        self.collection_name = 'square_payout_entries'
        self.collection_name_raw = 'raw_square_payout_entries'
        logger.debug('collection_name: %s', self.collection_name)
        super().__init__(config_name=self.collection_name)

        # initialize MongoDB collection
        self.collection = self.mdb.read_collection(self.collection_name)
        self.collection_raw = self.mdb.read_collection(self.collection_name_raw)
        self.location_ids = os.environ.get('SQUARE_LOCATIONS').split(',')

    def pull(self, payout_id: str) -> None:
        """Retrieves/processes a set of Square Payout Entries and saves them in MongoDB.

        Args:
            payout_id (str): Payout identifier.
        """
        # read the payout entries
        payout_entries = self.read(payout_id)

        # end if no payout entries
        if not payout_entries or len(payout_entries) == 0:
            logger.info('payout entries processed: 0')
            return

        update_count = 0
        for payout in tqdm(payout_entries, desc='payout_entries'):
            # update payout
            self.update_payout_entry(payout)
            update_count += 1

            # debug, only process one payout
            # break

        logger.info('payout entries processed: %s', update_count)

    def read(self, payout_id: str) -> list:
        """Returns a set of Square Payout Entries.

        Args:
            payout_id (str): Payout identifier.

        Returns:
            list: List of Square Payout Entries.
        """
        payout_entries = []

        result = self.api_payouts.list_payout_entries(
            payout_id=payout_id
        )

        if result.is_success():
            while result.body != {}:
                payout_entries.extend(result.body['payout_entries'])

                if result.cursor:
                    result = self.api_payouts.list_payout_entries(
                        payout_id=payout_id,
                        cursor=result.cursor
                    )
                else:
                    break

        elif result.is_error():
            for error in result.errors:
                logger.error(
                    'error reading payout entries: %s, %s\n%s',
                    error['category'], error['code'], error['detail']
                )

        return payout_entries

    def update_payout_entry(self, payout_entry):
        """Save the provided Square Object into MongoDB.

        Args:
            payout: Square Payout Entry object

        Returns:
            The MongoDB representation of the Square Payout Entry object.
        """
        self.decode_payout_entry(payout_entry)

        # get properties
        payout_entry_id = payout_entry['_id'] = payout_entry['id']

        # log the update
        # logger.debug('update_payout %s', payout_entry_id)

        # apply payout customizations
        self.apply_payout_entry_customizations(payout_entry)

        # update the database
        self.collection.find_one_and_replace(
            filter={'_id': payout_entry_id},
            replacement=payout_entry,
            upsert=True
        )

        return payout_entry

    def apply_payout_entry_customizations(self, payout_entry: dict):
        """Apply customizations to the Square Payout Entry object.

        This method should be overridden to apply app-specific customizations.
        The Square Payout object is modified directly.

        Args:
            payout_entry (dict): Square Payout Entry object
        """
        # pylint: disable=unused-argument

        # logger.debug('Applying default customizations: %s', self.collection_name)

        return

if __name__ == '__main__':
    # setup logging
    logger = Logger(__name__).get_logger()

    # collection = 'square_payouts'
    logger.info('working')

    square_payouts = Payouts()
    # square_payouts.mdb.square_payout_itemizations.drop()
    square_payouts.pull(begin_str='2024-03-05', thru_str='2024-03-11')
