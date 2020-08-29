"""Test functions for i_square.py.
"""
from datetime import datetime
import os

from dotenv import load_dotenv
import pytest

from squaredown.i_square import SquareInterface


@pytest.fixture
def square_iface():
    """Pytest fixture to initialize and return the SquareInterface object.
    """
    load_dotenv()

    return SquareInterface()

def test_init_square(square_iface):
    # test authentication
    square = square_iface.square_client
    api_locations = square.locations
    result = api_locations.list_locations()
    assert result.is_success()

    # test results
    square_locations = os.environ.get('SQUARE_LOCATIONS').split(',')
    found = False
    for loc in result.body['locations']:
        if loc['id'] in square_locations:
            found = True
            break

    assert found

def test_decode_order(square_iface):
    order = {
        'id': '110DevtQKzovAih4SVcVphyeV',
        'created_at': '2016-09-04T23:59:33.123Z',
        'updated_at': '2016-09-04T23:59:33.123Z',
    }

    square_iface.decode_order(order)

    assert type(order['created_at']) is datetime
    assert type(order['updated_at']) is datetime

def test_decode_datetime(square_iface):
    ref_dt_str = '2016-09-04T23:59:33.123Z'
    ref_dt = square_iface.decode_datetime(ref_dt_str)

    assert type(ref_dt) is datetime
    assert ref_dt.isoformat(timespec='milliseconds')[0:23] == ref_dt_str[0:23]
    assert ref_dt.isoformat(timespec='milliseconds')[-6:] == '+00:00'