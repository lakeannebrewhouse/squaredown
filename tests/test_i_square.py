"""Test functions for i_square.py.
"""
from datetime import datetime
import os

import pytest

from squaredown.i_square import SquareInterface


@pytest.fixture(name='square_if')
def fixture_square_iface():
    """Pytest fixture to initialize and return the SquareInterface object.
    """
    # logger.debug(f'using fixture "{name}"')
    return SquareInterface()

def test_init_square(square_if):
    """Tests Square Interface initialization.
    """

    # test authentication
    square = square_if.square_client
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

def test_decode_order(square_if):
    """Tests order decoding.
    """
    order = {
        'id': '110DevtQKzovAih4SVcVphyeV',
        'created_at': '2016-09-04T23:59:33.123Z',
        'updated_at': '2016-09-04T23:59:33.123Z',
    }

    square_if.decode_order(order)

    assert isinstance(order['created_at'], datetime)
    assert isinstance(order['updated_at'], datetime)

def test_decode_datetime(square_if):
    """Tests datetime decoding.
    """
    ref_dt_str = '2016-09-04T23:59:33.123Z'
    ref_dt = square_if.decode_datetime(ref_dt_str)

    assert isinstance(ref_dt, datetime)
    assert ref_dt.isoformat(timespec='milliseconds')[0:23] == ref_dt_str[0:23]
    assert ref_dt.isoformat(timespec='milliseconds')[-6:] == '+00:00'
