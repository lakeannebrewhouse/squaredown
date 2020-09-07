"""Test functions for i_mongo.py.
"""
from datetime import datetime
from dateutil import tz, utils

import pytest

import squaredown as sqd

# initialize module variables
DB_NAME = '_testdb'
DT_NAIVE = datetime(2020, 8, 24, 11, 23)
DT_UTC = utils.default_tzinfo(DT_NAIVE, tz.UTC)
DT_LOCAL = utils.default_tzinfo(DT_NAIVE, tz.tzlocal())


@pytest.fixture(name='mongodb_if')
def fixture_mongodb_interface():
    """Pytest fixture to initialize and return the MongoDBInterface object.
    """
    return sqd.i_mongodb.MongoDBInterface(db_name=DB_NAME)

def test_init_mongodb(mongodb_if):
    """Tests MongoDB initialization.
    """
    mdb = mongodb_if.mdb
    assert mdb

    # verify database name
    assert mdb.name == DB_NAME

def test_create_collection(mongodb_if):
    """Tests collection creation.
    """
    collection_name = '_test'
    collection = mongodb_if.create_collection(collection_name)
    assert collection.name == collection_name

    # verify that the collection was created
    collection_name_list = mongodb_if.mdb.list_collection_names()
    assert collection_name in collection_name_list

def test_read_collection(mongodb_if):
    """Tests collection read.
    """
    collection_name = '_test'
    collection = mongodb_if.read_collection(collection_name)
    assert collection.name == collection_name

@pytest.fixture(name='test_collection')
def fixture_datetime_test_collection(mongodb_if):
    """Pytest fixture to set a document with different datetime formats.

    Takes the mongodb_if fixture and returns a collection
    """
    doc_write = {
        '_id': 'test_datetime',
        'datetime_naive': DT_NAIVE,
        'datetime_utc': DT_UTC,
        'datetime_local': DT_LOCAL
    }

    collection = mongodb_if.mdb['_test']
    collection.find_one_and_replace(
        filter={'_id': 'test_datetime'},
        replacement=doc_write,
        upsert=True)

    return collection

def test_read_datetime_tz_utc(test_collection):
    """Tests reading UTC datetime from MongoDB.

    UTC datetime objects stored in MongoDB are retrieved naive.
    """
    doc_read = test_collection.find_one({'_id': 'test_datetime'})
    dt_read = doc_read['datetime_utc']

    assert dt_read == DT_UTC

def test_read_datetime_tz_local(test_collection):
    """Tests reading local datetime from MongoDB.

    UTC datetime objects stored in MongoDB are retrieved naive.
    """
    doc_read = test_collection.find_one({'_id': 'test_datetime'})
    dt_read = doc_read['datetime_local']

    assert dt_read == DT_LOCAL

def test_read_datetime_tz_naive(test_collection):
    """Tests reading timezone-naive datetime from MongoDB.

    UTC datetime objects stored in MongoDB are retrieved naive.
    """
    doc_read = test_collection.find_one({'_id': 'test_datetime'})
    dt_read = doc_read['datetime_naive']

    assert dt_read == DT_UTC

def test_delete_collection(mongodb_if):
    """Tests collection deletion.
    """
    collection_name = '_test'
    mongodb_if.delete_collection(collection_name)

    # verify that the collection was deleted
    collection_name_list = mongodb_if.mdb.list_collection_names()
    assert collection_name not in collection_name_list
