"""Class module to interface with Square.
"""
import os

from aracnid_logger import Logger
from dateutil import tz, utils
from dateutil.parser import parse
from square.client import Client

# initialize logging
logger = Logger(__name__).get_logger()


class SquareInterface:
    """Interface to Square.

    Environment Variables:
        SQUARE_ACCESS_TOKEN: OAuth 2.0 Access Token for Square API
        SQUARE_ENV: Square API Environment

    Attributes:
        api_orders: Square client to the Orders API.
        api_payments: Square client to the Payments API.
        api_refunds: Square client to the Refunds API.
        square_client: The Square Client.
    """

    def __init__(self):
        """Initializes the Square interface.

        Args:
            square_client: The Square Client.
        """
        square_access_token = os.environ.get('SQUARE_ACCESS_TOKEN')
        square_environment = os.environ.get('SQUARE_ENV')
        self.square_client = Client(
            access_token=square_access_token,
            environment=square_environment
        )

        self.api_orders = self.square_client.orders
        self.api_payments = self.square_client.payments
        self.api_refunds = self.square_client.refunds

    def decode_order(self, order):
        """Decodes a Square Order into a python dictionary.

        Square represents timestamps as RFC 3339 strings. This method decodes
        these strings into localized datetime objects.

        Args:
            order: The Square Order object.
        """
        if 'created_at' in order:
            order['created_at'] = self.decode_datetime(
                order['created_at'])
        if 'updated_at' in order:
            order['updated_at'] = self.decode_datetime(
                order['updated_at'])
        if 'closed_at' in order:
            order['closed_at'] = self.decode_datetime(
                order['closed_at'])
        if 'fulfillments' in order:
            for fulfillment in order['fulfillments']:
                self.decode_fulfillment(fulfillment)
        if 'tenders' in order:
            for tender in order['tenders']:
                self.decode_tender(tender)
        if 'refunds' in order:
            for refund in order['refunds']:
                self.decode_refund(refund)

    def decode_fulfillment(self, fulfillment):
        """Decodes a Square OrderFulfillment into a python dictionary.

        Square represents timestamps as RFC 3339 strings. This method decodes
        these strings into localized datetime objects.

        Args:
            fulfillment: The Square OrderFulfillment object.
        """
        if 'pickup_details' in fulfillment:
            pickup_details = fulfillment['pickup_details']
            self.decode_fulfillment_pickup(pickup_details)

        if 'shipment_details' in fulfillment:
            shipment_details = fulfillment['shipment_details']
            self.decode_fulfillment_shipment(shipment_details)

    def decode_fulfillment_pickup(self, pickup_details):
        """Decodes a Square OrderFulfillment pickup details.

        Square represents timestamps as RFC 3339 strings. This method decodes
        these strings into localized datetime objects.

        Args:
            pickup_details: The Square OrderFulfillment pickup details.
        """
        if 'accepted_at' in pickup_details:
            pickup_details['accepted_at'] = self.decode_datetime(
                pickup_details['accepted_at'])
        if 'canceled_at' in pickup_details:
            pickup_details['canceled_at'] = self.decode_datetime(
                pickup_details['canceled_at'])
        if 'curbside_pickup_details' in pickup_details:
            curbside_pickup_details = pickup_details['curbside_pickup_details']
            curbside_pickup_details['buyer_arrived_at'] = self.decode_datetime(
                curbside_pickup_details['buyer_arrived_at'])
        if 'expired_at' in pickup_details:
            pickup_details['expired_at'] = self.decode_datetime(
                pickup_details['expired_at'])
        if 'picked_up_at' in pickup_details:
            pickup_details['picked_up_at'] = self.decode_datetime(
                pickup_details['picked_up_at'])
        if 'pickup_at' in pickup_details:
            pickup_details['pickup_at'] = self.decode_datetime(
                pickup_details['pickup_at'])
        if 'placed_at' in pickup_details:
            pickup_details['placed_at'] = self.decode_datetime(
                pickup_details['placed_at'])
        if 'ready_at' in pickup_details:
            pickup_details['ready_at'] = self.decode_datetime(
                pickup_details['ready_at'])
        if 'rejected_at' in pickup_details:
            pickup_details['rejected_at'] = self.decode_datetime(
                pickup_details['rejected_at'])

    def decode_fulfillment_shipment(self, shipment_details):
        """Decodes a Square OrderFulfillment shipment details.

        Square represents timestamps as RFC 3339 strings. This method decodes
        these strings into localized datetime objects.

        Args:
            shipment_details: The Square OrderFulfillment shipment details.
        """
        if 'canceled_at' in shipment_details:
            shipment_details['canceled_at'] = self.decode_datetime(
                shipment_details['canceled_at'])
        if 'expected_shipped_at' in shipment_details:
            shipment_details['expected_shipped_at'] = self.decode_datetime(
                shipment_details['expected_shipped_at'])
        if 'failed_at' in shipment_details:
            shipment_details['failed_at'] = self.decode_datetime(
                shipment_details['failed_at'])
        if 'in_progress_at' in shipment_details:
            shipment_details['in_progress_at'] = self.decode_datetime(
                shipment_details['in_progress_at'])
        if 'packaged_at' in shipment_details:
            shipment_details['packaged_at'] = self.decode_datetime(
                shipment_details['packaged_at'])
        if 'placed_at' in shipment_details:
            shipment_details['placed_at'] = self.decode_datetime(
                shipment_details['placed_at'])
        if 'shipped_at' in shipment_details:
            shipment_details['shipped_at'] = self.decode_datetime(
                shipment_details['shipped_at'])

    def decode_tender(self, tender):
        """Decodes a Square Tender into a python dictionary.

        Square represents timestamps as RFC 3339 strings. This method decodes
        these strings into localized datetime objects.

        Args:
            tender: The Square Tender object.
        """
        if 'created_at' in tender:
            tender['created_at'] = self.decode_datetime(
                tender['created_at'])

    def decode_payment(self, payment):
        """Decodes a Square Payment into a python dictionary.

        Square represents timestamps as RFC 3339 strings. This method decodes
        these strings into localized datetime objects.

        Args:
            payment: The Square Payment object.
        """
        if 'created_at' in payment:
            payment['created_at'] = self.decode_datetime(
                payment['created_at'])
        if 'updated_at' in payment:
            payment['updated_at'] = self.decode_datetime(
                payment['updated_at'])
        if 'processing_fee' in payment:
            for fee in payment['processing_fee']:
                if 'effective_at' in fee:
                    fee['effective_at'] = self.decode_datetime(
                        fee['effective_at'])
        if 'delayed_until' in payment:
            payment['delayed_until'] = self.decode_datetime(
                payment['delayed_until'])

    def decode_refund(self, refund):
        """Decodes a Square PaymentRefund into a python dictionary.

        Square represents timestamps as RFC 3339 strings. This method decodes
        these strings into localized datetime objects.

        Args:
            refund: The Square PaymentRefund object.
        """
        if 'created_at' in refund:
            refund['created_at'] = self.decode_datetime(
                refund['created_at'])
        if 'updated_at' in refund:
            refund['updated_at'] = self.decode_datetime(
                refund['updated_at'])
        if 'processing_fee' in refund:
            for fee in refund['processing_fee']:
                if 'effective_at' in fee:
                    fee['effective_at'] = self.decode_datetime(
                        fee['effective_at'])

    @staticmethod
    def decode_datetime(dt_str):
        """Decodes a Square datetime string into a datetime object

        The datetime.fromisoformat() class method does not handle "Z" timezone
        notation, so the default_tzinfo() method is used instead.

        Args:
            dt_str: Datetime string to decode.
        """
        return utils.default_tzinfo(parse(dt_str), tz.tzlocal())


    def search(self, obj_type, search_filter):
        """Retrieves a list of filtered Square objects.

        Args:
            obj_type: Type of Square object to search, e.g., 'orders', 'items', etc.
            search_filter: Search filter

        Returns:
            List of Square objects that meet the filter criteria.
        """
        obj_list = []

        # get the api for the object
        api_type = None
        if obj_type == 'orders':
            api_type = self.square_client.orders

        if not api_type:
            return obj_list

        loop_count = 0
        result = self.search_fn(obj_type, search_filter)
        if result.is_success():
            loop_count += 1
            obj_list = result.body[obj_type]

            # process remaining pages
            cursor = result.body['cursor'] if 'cursor' in result.body else None
            while cursor:
                search_filter['cursor'] = cursor
                result = self.search_fn(obj_type, search_filter)

                if result.is_success():
                    loop_count += 1
                    obj_list.extend(result.body[obj_type])
                    cursor = result.body['cursor'] if 'cursor' in result.body else None
                elif result.is_error():
                    logger.error(f'Error calling OrdersApi.search_orders: {loop_count}')
                    logger.error(result.errors)

        elif result.is_error():
            logger.error(f'Error calling OrdersApi.search_orders: loop {loop_count}')
            logger.error(result.errors)

        return obj_list

    def search_fn(self, obj_type, search_filter):
        """Executes the search function for the specified "obj_type".

        Args:
            obj_type: Type of Square object to search, e.g., 'orders', 'items', etc.
            search_filter: Search filter

        Returns:
            Result of the search function.
        """
        if obj_type == 'orders':
            return self.api_orders.search_orders(search_filter)

        return None
