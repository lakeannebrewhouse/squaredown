"""A set of functions to retrieve and save Square data into MongoDB.
"""
from importlib.metadata import version

from squaredown.catalog import Catalog
from squaredown.connector import Connector
from squaredown.locations import Locations
from squaredown.orders import Orders


__version__ = version(__package__)
