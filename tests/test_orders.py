"""Test functions for orders.py.
"""
from dotenv import load_dotenv

import squaredown as sqd

def test_init_orders():
    """Tests that Orders() initializes successfully.
    """
    load_dotenv()

    orders = sqd.Orders()

    assert orders
    assert orders.api_orders
    assert orders.collection
    assert orders.location_ids
    assert orders.props
