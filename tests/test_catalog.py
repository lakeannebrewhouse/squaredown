"""Test functions for catalog.py.
"""
import squaredown as sqd

def test_init_catalog():
    """Tests that Catalog() initializes successfully.
    """
    catalog = sqd.Catalog('square_catalog_items')

    assert catalog
    assert catalog.api_catalog
    assert catalog.collection
    assert catalog.props
