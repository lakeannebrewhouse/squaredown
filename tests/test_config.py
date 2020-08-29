"""Test functions for config.py.
"""
import os

from datetime import datetime
from dotenv import load_dotenv
import pytest
from pytz import utc

# initialize module variables
test_props_name = '_test_props'


@pytest.fixture
def config_obj():
    """Pytest fixture to initialize and return a Config object
    """
    import squaredown as sqd

    load_dotenv()

    return sqd.config.Config()

@pytest.fixture
def config_props(config_obj):
    """Pytest fixture to initialize and load a configuration set.
    """
    config_obj.load_properties(test_props_name)
    return config_obj

def test_init_config_db(config_obj):
    config_db = config_obj.db

    assert config_db
    assert config_db.name == os.environ.get('MONGODB_DBNAME')

def test_init_config_collection(config_obj):
    config_collection = os.environ.get('SQUAREDOWN_CONFIG')
    assert config_obj._collection_name == config_collection

    assert config_obj._collection
    assert config_obj._collection.name == config_collection

def test_load_properties(config_props):
    assert config_props.name == test_props_name

def test_create_update_and_read_property(config_props):
    config_props.newprop = 'abc'
    config_props.update()
    props = config_props.props
    assert props['newprop'] == 'abc'

    config_collection = config_props._collection
    props = config_collection.find_one({'_id': test_props_name})['props']
    assert props['newprop'] == 'abc'

def test_create_update_and_read_datetime_property(config_props):
    config_props.auto_update = False
    dt = utc.localize(datetime(2020, 8, 24, 10, 50))
    config_props.prop_attr = dt
    # config_props.update()
    props = config_props.props
    assert config_props.prop_attr == dt

    config_collection = config_props._collection
    props = config_collection.find_one({'_id': test_props_name})['props']
    with pytest.raises(KeyError) as excinfo:
        var = props['prop_attr']

    assert 'prop_attr' in str(excinfo.value)

def test_create_update_and_read_datetime_property_auto_update(config_props):
    dt = utc.localize(datetime(2020, 8, 24, 10, 50))
    config_props.prop_attr_auto = dt
    props = config_props.props
    assert config_props.prop_attr_auto == dt

    config_collection = config_props._collection
    props = config_collection.find_one({'_id': test_props_name})['props']
    assert props['prop_attr_auto'] == dt

def test_create_update_and_read_datetime_property_subscripted(config_props):
    config_props.auto_update = False
    dt = utc.localize(datetime(2020, 8, 24, 10, 50))
    config_props['sub'] = dt
    # config_props.update()
    props = config_props.props
    assert config_props['sub'] == dt

    config_collection = config_props._collection
    props = config_collection.find_one({'_id': test_props_name})['props']
    with pytest.raises(KeyError) as excinfo:
        var = props['sub']

    assert 'sub' in str(excinfo.value)

def test_create_update_and_read_datetime_property_subscripted_auto_update(config_props):
    dt = utc.localize(datetime(2020, 8, 24, 10, 50))
    config_props['sub_auto'] = dt
    props = config_props.props
    assert config_props['sub_auto'] == dt

    config_collection = config_props._collection
    props = config_collection.find_one({'_id': test_props_name})['props']
    assert props['sub_auto'] == dt

def test_delete_property(config_props):
    config_collection = config_props._collection
    props = config_collection.find_one({'_id': test_props_name})['props']
    count_previous = len(props)

    del config_props.newprop
    config_props.update()

    props = config_collection.find_one({'_id': test_props_name})['props']
    count_deleted = len(props)

    assert count_previous == count_deleted + 1

def test_delete_properties(config_props):
    config_collection = config_props._collection
    count_previous = config_collection.count_documents({})

    config_props.delete()

    count_after_deleted = config_collection.count_documents({})

    assert count_previous == count_after_deleted + 1
