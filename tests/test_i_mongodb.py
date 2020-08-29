"""Test functions for i_mongo.py.
"""
from datetime import datetime
from dateutil import tz, utils
from pprint import pprint

from bson.objectid import ObjectId
from dotenv import load_dotenv
import pytest
from pytz import utc

import squaredown as sqd

# initialize module variables
db_name = '_testdb'
dt_naive = datetime(2020, 8, 24, 11, 23)
dt_utc = utils.default_tzinfo(dt_naive, tz.UTC)
dt_local = utils.default_tzinfo(dt_naive, tz.tzlocal())


@pytest.fixture
def mdb_iface():
    """Pytest fixture to initialize and return the MongoDBInterface object.
    """
    load_dotenv()

    return sqd.i_mongodb.MongoDBInterface(db_name=db_name)

def test_init_mongodb(mdb_iface):
    mdb = mdb_iface.db
    assert mdb

    # verify database name
    assert mdb.name == db_name

def test_create_collection(mdb_iface):
    collection_name = '_test'
    collection = mdb_iface.create_collection(collection_name)
    assert collection.name == collection_name

    # verify that the collection was created
    collection_name_list = mdb_iface.db.list_collection_names()
    assert collection_name in collection_name_list  

def test_read_collection(mdb_iface):
    collection_name = '_test'
    collection = mdb_iface.read_collection(collection_name)
    assert collection.name == collection_name

@pytest.fixture
def mdb_datetime_test_collection(mdb_iface):
    """Pytest fixture to set a document with different datetime formats.

    Takes the mdb_iface fixture and returns a collection
    """
    doc_write = {
        '_id': 'test_datetime',
        'datetime_naive': dt_naive,
        'datetime_utc': dt_utc,
        'datetime_local': dt_local
    }

    collection = mdb_iface.db['_test']
    result = collection.find_one_and_replace(
        filter={'_id': 'test_datetime'}, 
        replacement=doc_write, 
        upsert=True)

    return collection

def test_read_datetime_tz_utc(mdb_datetime_test_collection):
    """UTC datetime objects stored in MongoDB are retrieved naive.
    """
    doc_read = mdb_datetime_test_collection.find_one({'_id': 'test_datetime'})
    dt_read = doc_read['datetime_utc']

    assert dt_read == dt_utc

def test_read_datetime_tz_local(mdb_datetime_test_collection):
    """UTC datetime objects stored in MongoDB are retrieved naive.
    """
    doc_read = mdb_datetime_test_collection.find_one({'_id': 'test_datetime'})
    dt_read = doc_read['datetime_local']

    assert dt_read == dt_local

def test_read_datetime_tz_naive(mdb_datetime_test_collection):
    """UTC datetime objects stored in MongoDB are retrieved naive.
    """
    doc_read = mdb_datetime_test_collection.find_one({'_id': 'test_datetime'})
    dt_read = doc_read['datetime_naive']

    assert dt_read == dt_utc

def test_delete_collection(mdb_iface):
    collection_name = '_test'
    mdb_iface.delete_collection(collection_name)

    # verify that the collection was deleted
    collection_name_list = mdb_iface.db.list_collection_names()
    assert collection_name not in collection_name_list

