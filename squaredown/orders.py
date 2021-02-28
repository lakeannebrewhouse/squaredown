"""Orders class module for Squaredown.
"""
import os

from aracnid_logger import Logger
from tqdm import tqdm

from squaredown.connector import Connector
from squaredown.itemizations import Itemizations

# initialize logging
logger = Logger(__name__).get_logger()


class Orders(Connector):
    """Contains the code to connect and pull orders from Square to MongoDB.

    Environment Variables:
        SQUARE_LOCATIONS: List of Square Locations to process.

    Attributes:
        collection: Square Orders collection in MongoDB.
        collection_name: Name of the Square Orders collection in MongoDB.
        location_ids: Square location identifiers.
    """

    def __init__(self):
        """Initializes the Orders Connector.

        Establishes connections to Square and MongoDB.
        Sets up access to configuration properties.

        """
        self.collection_name = 'square_orders'
        self.collection_name_raw = 'raw_square_orders'
        logger.debug(f'collection_name: {self.collection_name}')
        super().__init__(config_name=self.collection_name)

        # initialize MongoDB collection
        self.collection = self.read_collection(self.collection_name)
        self.collection_raw = self.read_collection(self.collection_name_raw)
        self.location_ids = os.environ.get('SQUARE_LOCATIONS').split(',')

        # initialize reference to Itemizations
        self.itemizations = Itemizations()

    def pull(self, save_last=True, from_raw=False, **kwargs):
        """Retrieves/processes a set of Square Orders and saves them in MongoDB.

        Args:
            save_last (bool): if set to True (default), details of the last
                object retrieved is saved in the configuration properties
            from_raw (bool): if set to True, reprocesses orders previously saved
                to raw MongoDB collection (default=False)
            **kwargs: keyword arguments that specify the timespan to retrieve

        Returns:
            None
        """
        # read the orders
        orders = self.read_orders(from_raw, **kwargs)

        update_count = 0
        for order in tqdm(orders, desc='orders'):
            # save the raw order
            if not from_raw:
                self.save_raw_order(order)

            # initialize order variables
            order_id = order['id']
            updated_at = self.decode_datetime(order['updated_at'])

            # check last updated object for duplicate
            if update_count == 0:
                if order_id == self.props.last_id:
                    if updated_at == self.props.last_updated:
                        continue

            # update order
            self.update_order(order)
            update_count += 1

            # update config properties
            if save_last:
                self.props.last_updated = updated_at
                self.props.last_id = order_id
                self.props.update()

            # debug, only process one order
            # break

        logger.debug(f'orders processed: {update_count}')

    def read_orders(self, from_raw=False, **kwargs):
        """Returns a set of Square Orders.

        Args:
            from_raw (bool): if set to True, reprocesses orders previously saved
                to raw MongoDB collection (default=False)
            **kwargs: keyword arguments that specify the timespan to retrieve

        Returns:
            List of Square Orders.
        """
        start, end = self.timespan(collection='square_orders', **kwargs)
        logger.debug(f'timespan: {start}, {end}')

        if from_raw:
            mongodb_filter = {
                'updated_at': {
                    '$gte': start.isoformat(),
                    '$lt': end.isoformat()
                }
            }
            cursor = self.collection_raw.find(
                filter=mongodb_filter,
                sort=[('updated_at', 1)]
            )

            orders = list(cursor)

        else:
            square_filter = {
                'location_ids': self.location_ids,
                'query': {
                    'filter': {
                        'date_time_filter': {
                            'updated_at': {
                                'start_at': start.isoformat(),
                                'end_at': end.isoformat()
                            }
                        }
                    },
                    'sort': {
                        'sort_field': 'UPDATED_AT',
                        'sort_order': 'ASC'
                    }
                }
            }

            orders = self.search('orders', square_filter)

        return orders        

    def save_raw_order(self, order):
        """Save the provided raw Square Object into MongoDB.

        Args:
            order: Raw Square Order object

        Returns:
            The MongoDB representation of the raw Square Order object.
        """
        # get order properties
        order_id = order['_id'] = order['id']

        # log the update
        logger.debug(f'{order_id}')

        # update the database
        self.collection_raw.find_one_and_replace(
            filter={'_id': order_id},
            replacement=order,
            upsert=True
        )

        return order

    def update_order(self, order):
        """Save the provided Square Object into MongoDB.

        Args:
            order: Square Order object

        Returns:
            The MongoDB representation of the Square Order object.
        """
        self.decode_order(order)

        # get order properties
        order_id = order['_id'] = order['id']
        updated_at = order.get('updated_at')

        # log the update
        logger.info(f'update_order {order_id}: '
                    f'{updated_at.isoformat()[0:16]}')

        # get the order state (with overrides)
        state = self.get_order_state(order)
        order['state'] = state

        # apply order customizations
        self.apply_order_customizations(order)

        # process property blocks
        self.process_tenders(order)
        self.process_fulfillments(order)
        self.process_itemizations(order)
        self.process_refunds(order)
        self.process_returns(order)

        # update the database
        try:
            self.collection.find_one_and_replace(
                filter={
                    '_id': order_id,
                    '$or': [{'_fixed': {'$exists': 0}}, {'_fixed': False}]
                },
                replacement=order,
                upsert=True
            )
        except self.DuplicateKeyError:
            logger.warning(f'Attempted to update FIXED Order "{order_id}"')

        return order

    def apply_order_customizations(self, order):
        """Apply customizations to the Square Order object.

        This method should be overridden to apply app-specific customizations.
        The Square Order object is modified directly.

        Args:
            order: Square Order object

        Returns:
            None.
        """
        logger.debug(f'Applying default customizations: {self.collection_name}')

        # set the "source" property, default to PoS
        order['source'] = order.get('source', {'name': 'Point of Sale'})

    @staticmethod
    def get_order_state(order):
        """Returns the current state of the Square Order.

        The state comes directly from the 'state' property of the Order.
        The standard values are: OPEN, COMPLETED, CANCELED.
        Additional custom values were added to provide more details based on
        the 'status' of the Tender card details.

        If there are multiple tenders, the first non-CAPTURED status will be
        used to set the additional states.

        The additional states are:
        - OPEN_TENDER_AUTHORIZED
        - OPEN_TENDER_VOIDED
        - OPEN_TENDER_FAILED
        - OPEN_TENDER_MISSING

        Args:
            order: Square Order object.
        """
        order_id = order.get('id')
        state = order.get('state')
        tender_status = None

        if state and state == 'OPEN':
            tenders = order.get('tenders')
            if tenders:
                for tender in tenders:
                    tender_status = tender['card_details']['status']
                    if tender_status != 'CAPTURED':
                        state = f'OPEN_TENDER_{tender_status}'
                        logger.error(
                            f'Tender issue ({state}) in square order '
                            f'{order_id}')
                        break

            else:
                state = 'OPEN_TENDER_MISSING'
                logger.error(
                    f'Tender issue ({state}) in square order {order_id}')

        return state

    def process_tenders(self, order):
        """Processes tender data as a separate Square collection.

        Args:
            order: Square Order object
        """
        api_payments = self.square_client.payments

        tenders = order.get('tenders')
        if tenders:
            for tender in tenders:
                tender_id = tender['id']
                self.update_order_tender(tender, order)

                # process corresponding payments
                result = api_payments.get_payment(payment_id=tender_id)
                if result.is_success():
                    payment = result.body['payment']
                    self.update_payment(payment)
                elif result.is_error():
                    # no payment found, determine reason

                    # 100% discount
                    if tender['amount_money']['amount'] == 0:
                        pass
                    # check payments
                    elif tender['type'] == 'OTHER':
                        pass
                    # cash payments
                    elif tender['type'] == 'CASH':
                        pass
                    else:
                        logger.error('Error calling PaymentsApi.get_payment')
                        logger.error(result.errors)

    def update_order_tender(self, obj, order):
        """Updates MongoDB with the provided Square Tender object.

        Args:
            obj: Square Tender object.
            order: Square Order object.
        """
        collection_name = 'square_order_tenders'
        self.add_order_properties(obj, order)
        self.read_collection(collection_name).find_one_and_replace(
            {'_id': obj['id']}, obj, upsert=True)

    def update_payment(self, obj):
        """Updates MongoDB with the provided Square Payment object.

        Args:
            obj: Square Payment object.
        """
        collection_name = 'square_payments'
        self.decode_payment(obj)
        self.read_collection(collection_name).find_one_and_replace(
            {'_id': obj['id']}, obj, upsert=True)

    def process_fulfillments(self, order):
        """Processes fulfillment data as a separate Square collection.

        Args:
            order: Square Order object
        """
        if 'fulfillments' in order:
            fulfillments = order['fulfillments']

            for fulfillment in fulfillments:
                self.update_fulfillment(fulfillment, order)

    def update_fulfillment(self, obj, order):
        """Updates MongoDB with the provided Square Fulfillment object.

        Args:
            obj: Square Fulfillment object.
            order: Square Order object.
        """
        collection_name = 'square_order_fulfillments'
        self.add_order_properties(obj, order)
        self.read_collection(collection_name).find_one_and_replace(
            {'_id': obj['uid']}, obj, upsert=True)

    def process_itemizations(self, order):
        """Processes itemization data as a separate Square collection.

        Args:
            order: Square Order object
        """
        # make additional properties
        props = self.make_order_properties(order)
        props['itemization_type'] = 'sale'

        # process each line item
        line_items = order.get('line_items', [])
        for line_item in line_items:
            self.itemizations.update_itemization(line_item, order, props)

    def process_refunds(self, order):
        """Processes refund data as a separate Square collection.

        Args:
            order: Square Order object
        """
        order_id = order['_id']

        if 'refunds' in order:
            refunds = order['refunds']
            for refund in refunds:
                refund_tender_id = refund['tender_id']
                refund_id = '{}_{}'.format(refund_tender_id, refund['id'])

                result = self.api_refunds.get_payment_refund(
                    refund_id=refund_id)
                if result.is_success():
                    refund = result.body['refund']
                    self.update_refund(refund)
                elif result.is_error():
                    # no payment refund exists, determine reason
                    tender = self.mdb.square_order_tenders.find_one(
                        {'_id': refund_tender_id})

                    # cash or other tender refund
                    if tender and tender['type'] == 'CASH':
                        logger.debug(f'order: {order_id}, '
                            'no refund payment for "CASH" tender type')
                    elif tender and tender['type'] == 'OTHER':
                        logger.debug(f'order: {order_id}, '
                            'no refund payment for "OTHER" tender type')
                    else:
                        error_detail = result.errors[0]['detail']
                        logger.error(f'order: {order_id}, {error_detail}')

    def update_refund(self, obj):
        """Updates MongoDB with the provided Square Refund object.

        Args:
            obj: The Square Refund object.
        """
        collection_name = 'square_refunds'
        self.decode_refund(obj)
        self.read_collection(collection_name).find_one_and_replace(
            {'_id': obj['id']}, obj, upsert=True)

    def process_returns(self, order):
        """Processes return data as a separate Square collection.

        Args:
            order: Square Order object
        """
        # process each return
        returns = order.get('returns', [])
        for return_obj in returns:
            # make additional properties
            props = self.make_order_properties(order)
            props['itemization_type'] = 'return'
            props['source_order_id'] = return_obj['source_order_id']

            # process each return line item
            return_line_items = return_obj.get('return_line_items', [])
            for return_line_item in return_line_items:
                self.itemizations.update_itemization(
                    return_line_item, order, props)

    def add_order_properties(self, obj, order):
        """Adds additional properties to the object from the Order.

        Args:
            obj: Square object to apply order properties.
            order: Square Order object.
        """
        props = self.make_order_properties(order)
        obj.update(props)

    def make_order_properties(self, order):
        props = {
            'order_id': order['id'],
            'order_state': self.get_order_state(order),
            'order_created_at': order.get('created_at'),
            'order_updated_at': order.get('updated_at'),
            'order_location_id': order.get('location_id')
        }

        return props

if __name__ == '__main__':
    # setup logging
    logger = Logger(__name__).get_logger()

    # collection = 'square_orders'
    logger.info('working')

    square_orders = Orders()
    # square_orders.mdb.square_order_itemizations.drop()
    square_orders.pull(from_raw=True, begin_str='2016-01-01', thru_str='2016-03-31')
