"""Test functions for connector.py.
"""
from datetime import datetime
import os

import pytest

import squaredown as sqd


REF_BEGIN_STR = '2020-06-01T00:00:00-04:00'
REF_THRU_STR = '2020-06-08T00:00:00-04:00'


@pytest.fixture(name='conn')
def fixture_connector():
    """Pytest fixture to initialize and return a Connector.
    """
    collection_name = 'square_orders'
    conn = sqd.Connector(collection_name)

    return conn

def test_init_connector_square(conn):
    """Tests the Connector's Square initialization.
    """
    # test authentication
    square = conn.square_client
    api_locations = square.locations
    result = api_locations.list_locations()
    assert result.is_success()

def test_init_connector_mongodb(conn):
    """Tests the Connector's MongoDB initialization.
    """
    # test authentication
    mdb = conn.mdb
    assert mdb

    # verify database name
    db_name = os.environ.get('MONGODB_DBNAME')
    assert mdb.name == db_name

def test_timespan_args_begin_str_and_thru_str(conn):
    """Tests Connector's timespan arguments: begin_str, thru_str.
    """
    start, end = conn.timespan(begin_str=REF_BEGIN_STR, thru_str=REF_THRU_STR)

    assert isinstance(start, datetime)
    assert start.isoformat() == REF_BEGIN_STR

    assert isinstance(end, datetime)
    assert end.isoformat() == REF_THRU_STR

def test_timespan_args_begin_str_missing_and_config():
    """Tests Connector's timespan arguments: begin_str missing with config
    """
    collection_name = '_test_collection_with_last_updated'
    conn = sqd.Connector(collection_name)
    last_updated = datetime(2019, 8, 5, 12, 34).astimezone()
    conn.props.last_updated = last_updated
    conn.props.update()

    start, _ = conn.timespan(thru_str=REF_THRU_STR)
    last_updated = conn.props.last_updated

    assert isinstance(start, datetime)
    assert start == last_updated

def test_timespan_args_begin_str_missing_and_no_config():
    """Tests Connector's timespan arguments: begin_str missing with no config
    """
    collection_name = '_test_collection'
    conn = sqd.Connector(collection_name)

    start, _ = conn.timespan(thru_str=REF_THRU_STR)
    start_first = datetime(2016, 1, 1, 0, 0).astimezone()

    assert isinstance(start, datetime)
    assert start.isoformat() == start_first.isoformat()
