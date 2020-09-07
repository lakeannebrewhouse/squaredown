"""Test functions for orders.py.
"""
import squaredown as sqd

def test_init_orders():
    """Tests that Orders() initializes successfully.
    """
    orders = sqd.Orders()

    assert orders
    assert orders.api_orders
    assert orders.collection
    assert orders.location_ids
    assert orders.props
