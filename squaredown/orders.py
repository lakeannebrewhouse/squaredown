"""Orders class module for Squaredown.
"""
import os

from aracnid_logger import Logger

from squaredown.connector import Connector

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
        logger.debug(f'collection_name: {self.collection_name}')
        super().__init__(config_name=self.collection_name)

        # initialize MongoDB collection
        self.collection = self.read_collection(self.collection_name)
        self.location_ids = os.environ.get('SQUARE_LOCATIONS').split(',')

    def pull(self, save_last=True, **kwargs):
        """Retrieves a set of Square Orders and saves them in MongoDB.

        Args:
            save_last (bool): if set to True (default), details of the last
                object retrieved is saved in the configuration properties
            **kwargs: keyword arguments that specify the timespan to retrieve

        Returns:
            None
        """
        start, end = self.timespan(collection='square_orders', **kwargs)
        logger.debug(f'timespan: {start}, {end}')

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

        update_count = 0
        if orders:
            for order in orders:
                order_id = order['id']
                updated_at = self.decode_datetime(order['updated_at'])

                # check last updated object for duplicate
                if update_count == 0:
                    if order_id == self.props.last_id:
                        if updated_at == self.props.last_updated:
                            continue

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
            self.mdb.square_orders.find_one_and_replace(
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

        The additional states are:
        - OPEN_TENDER_AUTHORIZED
        - OPEN_TENDER_VOIDED
        - OPEN_TENDER_FAILED
        - OPEN_TENDER_MISSING
        - OPEN_TENDER_MULTIPLE_ERROR

        Args:
            order: Square Order object.
        """
        order_id = order.get('id')
        state = order.get('state')
        tender_status = None

        if state and state == 'OPEN':
            tenders = order.get('tenders')
            if tenders:
                if len(tenders) == 1:
                    tender_status = tenders[0]['card_details']['status']
                    if tender_status != 'CAPTURED':
                        state = f'OPEN_TENDER_{tender_status}'
                        logger.error(
                            f'Tender issue ({state}) in square order '
                            '{order_id}')
                else:
                    state = 'OPEN_TENDER_MULTIPLE_ERROR'
                    logger.error(
                        'Multiple tenders found while processing square order '
                        f'{order_id}')

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
        if 'line_items' in order:
            line_items = order['line_items']
            for line_item in line_items:
                self.update_order_itemization(line_item, order)

    def update_order_itemization(self, obj, order):
        """Updates MongoDB with the provided Square Itemization object.

        Args:
            obj: Square Itemization object.
            order: Square Order object.
        """
        collection_name = 'square_order_itemizations'
        self.add_order_properties(obj, order)
        self.read_collection(collection_name).find_one_and_replace(
            {'_id': obj['uid']}, obj, upsert=True)

    def process_refunds(self, order):
        """Processes refund data as a separate Square collection.

        Args:
            order: Square Order object
        """
        api_refunds = self.square_client.refunds

        if 'refunds' in order:
            refunds = order['refunds']
            for refund in refunds:
                refund_tender_id = refund['tender_id']
                refund_id = '{}_{}'.format(refund_tender_id, refund['id'])

                result = api_refunds.get_payment_refund(
                    refund_id=refund_id)
                if result.is_success():
                    refund = result.body['refund']
                    self.update_refund(refund)
                elif result.is_error():
                    # no payment refund exists, determine reason
                    tender = self.mdb.square_order_tenders.find_one(
                        {'_id': refund_tender_id})

                    # cash refund
                    if tender and tender['type'] == 'CASH':
                        pass
                    else:
                        logger.error(
                            'Error calling RefundsApi.get_payment_refund')
                        logger.error(result.errors)

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
        if 'returns' in order:
            returns = order['returns']
            for return_obj in returns:
                source_order_id = return_obj['source_order_id']
                return_line_items = return_obj['return_line_items']
                for return_line_item in return_line_items:
                    self.update_order_return_itemization(
                        return_line_item, order, source_order_id)

    def update_order_return_itemization(self, obj, order, source_order_id):
        """Updates MongoDB with the provided Square Return Itemization object.

        Args:
            obj: Square Return Itemization object.
            order: Square Order object.
        """
        collection_name = 'square_order_return_itemizations'
        self.add_order_properties(obj, order)
        obj['source_order_id'] = source_order_id
        self.read_collection(collection_name).find_one_and_replace(
            {'_id': obj['uid']}, obj, upsert=True)

    def add_order_properties(self, obj, order):
        """Adds additional properties to the object from the Order.

        Args:
            obj: Square Return Itemization object.
            order: Square Order object.
        """
        obj['order_id'] = order['id']
        obj['order_state'] = self.get_order_state(order)
        obj['order_created_at'] = order.get('created_at')
        obj['order_updated_at'] = order.get('updated_at')
