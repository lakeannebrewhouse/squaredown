"""Itemizations class module for Squaredown.
"""
from aracnid_logger import Logger

from squaredown.connector import Connector

# initialize logging
logger = Logger(__name__).get_logger()


class Itemizations(Connector):
    """Contains the code to process Square Order Itemizations.

    Attributes:
        collection: Square Order Itemizations collection in MongoDB.
        collection_name: Name of the Itemizations collection in MongoDB.
    """

    def __init__(self):
        """Initializes the Orders Connector.

        Establishes connections to Square and MongoDB.
        Sets up access to configuration properties.

        """
        self.collection_name = 'square_order_itemizations'
        logger.debug(f'collection_name: {self.collection_name}')
        super().__init__(config_name=self.collection_name)

        # initialize MongoDB collection
        self.collection = self.read_collection(self.collection_name)

    def save_raw_itemization(self, itemization, order):
        """Save the provided raw Square Object into MongoDB.

        The object identifier, obj_id, is the concatenation of the order
        id and the uid of the line item. The uid of the line item is not
        guaranteed to be unique across all orders.

        Args:
            itemization: Raw Square Itemization object
            order: Square Order object.

        Returns:
            The MongoDB representation of the raw Square Itemization object.
        """
        # get object properties
        order_id = order['_id']
        itemization_id = itemization['uid']
        obj_id = f'{order_id}_{itemization_id}'

        # log the update
        # logger.debug(f'{obj_id}')

        # update the database
        self.mdb.raw_square_order_itemizations.find_one_and_replace(
            filter={'_id': obj_id},
            replacement=itemization,
            upsert=True
        )

        return itemization

    def update_itemization(self, itemization, order, props):
        """Updates MongoDB with the provided Square Itemization object.

        The object identifier, obj_id, is the concatenation of the order
        id and the uid of the line item. The uid of the line item is not
        guaranteed to be unique across all orders.

        Args:
            itemization: Square Itemization object.
            order: Square Order object.
            props: Additional properties to set.
        """
        # save the raw itemization
        self.save_raw_itemization(itemization, order)

        # make the itemization identifier
        order_id = order['_id']
        uid = itemization['uid']
        itemization_id = f'{order_id}_{uid}'
        itemization['id'] = itemization_id

        # add additional properties
        itemization.update(props)

        # apply itemization customizations
        self.apply_itemization_customizations(itemization, order)

        # remove previous itemizations saved under uid
        self.collection.delete_one(
            filter={'_id': itemization['uid']})

        # save/replace itemization
        self.collection.find_one_and_replace(
            filter={'_id': itemization_id},
            replacement=itemization,
            upsert=True)

    def apply_itemization_customizations(self, itemization, order):
        """Apply customizations to the Square Order Itemization object.

        This method should be overridden to apply app-specific customizations.
        The Square Order Itemization object is modified directly.

        Args:
            itemization: Square Order Itemization object
            order: Square Order object

        Returns:
            None.
        """
        logger.debug(f'Applying default customizations: {self.collection_name}')

        # set the "source" property, default to PoS
        itemization['order_source'] = order['source']['name']

        # convert the quantity property to integer
        itemization['quantity'] = int(itemization['quantity'])
