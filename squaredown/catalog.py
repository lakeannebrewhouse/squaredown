"""Catalog class module for Squaredown.
"""
from aracnid_logger import Logger

from squaredown.connector import Connector

# initialize logging
logger = Logger(__name__).get_logger()


class Catalog(Connector):
    """Contains the code to connect and pull catalog items from Square
    to MongoDB.

    Environment Variables:
        None.

    Attributes:
        collection: Square Catalog collection in MongoDB.
        collection_name: Name of the Square Catalog collection in MongoDB.
        object_type: Object type of the Square Catalog object.
    """

    def __init__(self, collection_name):
        """Initializes the Catalog Connector.

        Establishes connections to Square and MongoDB.
        Sets up access to configuration properties.

        """
        self.collection_name = collection_name
        self.object_type = self.get_object_type(collection_name)
        logger.debug(f'collection_name: {self.collection_name}')
        super().__init__(config_name=self.collection_name)

        # initialize MongoDB collection
        self.collection = self.read_collection(self.collection_name)

    def pull(self, save_last=True, **kwargs):
        """Retrieves a set of Square objects for the specified `collection`
        and saves them in MongoDB.

        Args:
            collection: one of the following catalog names:
                'square_catalog_categories', 'square_catalog_items',
                'square_catalog_variations', 'square_catalog_taxes',
                'square_catalog_discounts', 'square_catalog_modifier_lists',
                'square_catalog_modifiers'
            save_last (bool): if set to True (default), details of the last
                object retrieved is saved in the configuration properties
            **kwargs: keyword arguments that specify the timespan to retrieve

        Returns:
            None
        """
        start, end = self.timespan(collection=self.collection_name, **kwargs)
        logger.debug(f'timespan: {start}, {end}')

        square_filter = {
            'object_types': [self.object_type],
            'begin_time': start.isoformat()
        }

        objects = self.search('objects', square_filter)

        update_count = 0
        if objects:
            for obj in sorted(objects, key=lambda o: o['updated_at']):
                obj_id = obj['id']
                updated_at = self.decode_datetime(obj['updated_at'])

                # check last updated object for duplicate
                if update_count == 0:
                    if obj_id == self.props.last_id:
                        if updated_at == self.props.last_updated:
                            continue

                self.update_obj(obj)
                update_count += 1

                # update config properties
                if save_last:
                    self.props.last_updated = updated_at
                    self.props.last_id = obj_id
                    self.props.update()

                # debug, only process one obj
                # break

        logger.debug(f'objects processed: {update_count}')

    def update_obj(self, obj):
        """Save the provided Square Catalog Object into MongoDB.

        Args:
            obj: Square Catalog object

        Returns:
            The MongoDB representation of the Square Catalog object
        """
        self.decode_catalog_obj(obj, self.collection_name)

        # get object properties
        obj_id = obj['_id'] = obj['id']
        updated_at = obj.get('updated_at')

        # log the update
        logger.info(f'update_catalog_object {obj_id} ({self.collection_name}): '
            f'{updated_at.isoformat()[0:16]}')

        # apply object customizations
        self.apply_object_customizations(obj)

        # save the object to mdb
        self.collection.find_one_and_replace(
            filter={'_id': obj_id},
            replacement=obj,
            upsert=True
        )

        return obj

    @staticmethod
    def get_object_type(collection_name):
        """Returns the Square Catalog object type for the specified collection.

        Args:
            collection_name: Name of the Square Catalog collection in MongoDB.

        Returns:
            Square Catalog object type.
        """
        object_type = None

        if collection_name == 'square_catalog_categories':
            object_type = 'CATEGORY'
        elif collection_name == 'square_catalog_items':
            object_type = 'ITEM'
        elif collection_name == 'square_catalog_item_variations':
            object_type = 'ITEM_VARIATION'
        elif collection_name == 'square_catalog_taxes':
            object_type = 'TAX'
        elif collection_name == 'square_catalog_discounts':
            object_type = 'DISCOUNT'
        elif collection_name == 'square_catalog_modifiers':
            object_type = 'MODIFIER'
        elif collection_name == 'square_catalog_modifier_lists':
            object_type = 'MODIFIER_LIST'

        return object_type

    def apply_object_customizations(self, obj):
        """Apply customizations to the Square Catalog object.

        This method should be overridden to apply app-specific customizations.
        The Square Catalog object is modified directly.

        Args:
            obj: Square Catalog object

        Returns:
            None.
        """
        logger.debug(f'Applying default customizations: {self.collection_name}')

        # flatten catalog data
        field_name = f'{self.object_type.lower()}_data'
        obj.update(obj[field_name])
        obj.pop(field_name)
