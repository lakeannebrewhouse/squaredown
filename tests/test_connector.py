"""Test functions for connector.py.
"""
from datetime import datetime
import os

from dotenv import load_dotenv
import pytest

import squaredown as sqd


@pytest.fixture
def conn():
    load_dotenv()

    collection_name = 'square_orders'
    conn = sqd.Connector(collection_name)

    return conn 

def test_init_connector_square(conn):
    # test authentication
    square = conn.square_client
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

def test_init_connector_mongodb(conn):
    # test authentication
    mdb = conn.db
    assert mdb

    # verify database name
    db_name = os.environ.get('MONGODB_DBNAME')
    assert mdb.name == db_name

ref_begin_str = '2020-06-01T00:00:00-04:00'
ref_thru_str = '2020-06-08T00:00:00-04:00'

def test_timespan_args_begin_str_and_thru_str(conn):
    start, end = conn.timespan(begin_str=ref_begin_str, thru_str=ref_thru_str)
    
    assert type(start) == datetime
    assert start.isoformat() == ref_begin_str

    assert type(end) == datetime
    assert end.isoformat() == ref_thru_str

def test_timespan_args_begin_str_missing_and_config():
    load_dotenv()
    collection_name = '_test_collection_with_last_updated'
    conn = sqd.Connector(collection_name)
    last_updated = datetime(2019, 8, 5, 12, 34).astimezone()
    conn.props.last_updated = last_updated
    conn.props.update()

    start, end = conn.timespan(thru_str=ref_thru_str)
    last_updated = conn.props.last_updated
    
    assert type(start) == datetime
    assert start == last_updated

def test_timespan_args_begin_str_missing_and_no_config():
    load_dotenv()

    collection_name = '_test_collection'
    conn = sqd.Connector(collection_name)

    start, end = conn.timespan(thru_str=ref_thru_str)
    start_first = datetime(2016, 1, 1, 0, 0).astimezone()
    
    assert type(start) == datetime
    assert start.isoformat() == start_first.isoformat()
