"""Test functions for datetime_utils.py.
"""
from datetime import datetime, timedelta
import os

from dotenv import load_dotenv
import pytest
from dateutil import tz

from squaredown.datetime_utils import timespan
import squaredown.config as cfg
import squaredown.i_mongodb as i_mongodb

# initialize module variables
ref_begin_str = '2020-06-01T00:00:00-04:00'
ref_thru_str = '2020-06-08T00:00:00-04:00'
ref_begin_iso = '2020-W10'
ref_thru_iso = '2020-W25'
collection = '_testprops'


def test_timespan_args_begin_str_and_thru_str():
    start, end = timespan(begin_str=ref_begin_str, thru_str=ref_thru_str)
    
    assert type(start) == datetime
    assert start.isoformat() == ref_begin_str

    assert type(end) == datetime
    assert end.isoformat() == ref_thru_str

def test_timespan_args_begin_str_and_thru_str_none():
    start, end = timespan(begin_str=ref_begin_str, thru_str=None)
    end_now = datetime.now(tz.tzlocal())
    
    assert type(start) == datetime
    assert start.isoformat() == ref_begin_str

    assert type(end) == datetime
    assert end.isoformat()[0:18] == end_now.isoformat()[0:18]

def test_timespan_args_begin_str_and_thru_str_missing():
    start, end = timespan(begin_str=ref_begin_str)
    end_now = datetime.now(tz.tzlocal())
    
    assert type(start) == datetime
    assert start.isoformat() == ref_begin_str

    assert type(end) == datetime
    assert end.isoformat()[0:18] == end_now.isoformat()[0:18]

def test_timespan_args_begin_str_none_and_thru_str():
    start, end = timespan(begin_str=None, thru_str=ref_thru_str)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()
    
    assert type(start) == datetime
    assert start.isoformat() == start_first.isoformat()

    assert type(end) == datetime
    assert end.isoformat() == ref_thru_str

def test_timespan_args_begin_str_missing_and_thru_str():
    start, end = timespan(thru_str=ref_thru_str)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()
    
    assert type(start) == datetime
    assert start.isoformat() == start_first.isoformat()

    assert type(end) == datetime
    assert end.isoformat() == ref_thru_str

def test_timespan_args_begin_and_thru():
    begin = datetime.fromisoformat(ref_begin_str)
    thru = datetime.fromisoformat(ref_thru_str)
    start, end = timespan(begin=begin, thru=thru)
    
    assert type(start) == datetime
    assert start == begin

    assert type(end) == datetime
    assert end == thru

def test_timespan_args_begin_and_thru_none():
    begin = datetime.fromisoformat(ref_begin_str)
    start, end = timespan(begin=begin, thru=None)
    thru_now = datetime.now(tz.tzlocal())
    
    assert type(start) == datetime
    assert start == begin

    assert type(end) == datetime
    assert end.isoformat()[0:18] == thru_now.isoformat()[0:18]

def test_timespan_args_begin_and_thru_missing():
    begin = datetime.fromisoformat(ref_begin_str)
    start, end = timespan(begin=begin)
    thru_now = datetime.now(tz.tzlocal())
    
    assert type(start) == datetime
    assert start == begin

    assert type(end) == datetime
    assert end.isoformat()[0:18] == thru_now.isoformat()[0:18]

def test_timespan_args_begin_none_and_thru():
    thru = datetime.fromisoformat(ref_thru_str)
    start, end = timespan(begin=None, thru=thru)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()
    
    assert type(start) == datetime
    assert start == start_first

    assert type(end) == datetime
    assert end == thru

def test_timespan_args_begin_missing_and_thru():
    thru = datetime.fromisoformat(ref_thru_str)
    start, end = timespan(thru=thru)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()
    
    assert type(start) == datetime
    assert start == start_first

    assert type(end) == datetime
    assert end == thru

def test_timespan_args_begin_iso_str_and_thru_iso_str():
    start, end = timespan(begin_str=ref_begin_iso, thru_str=ref_thru_iso)
    begin = datetime.fromisocalendar(2020, 10, 1).astimezone()
    thru = datetime.fromisocalendar(2020, 25, 1).astimezone() + timedelta(days=7)
    
    assert type(start) == datetime
    assert start == begin

    assert type(end) == datetime
    assert end == thru

def test_timespan_args_begin_iso_and_thru_str_none():
    start, end = timespan(begin_str=ref_begin_iso, thru_str=None)
    begin = datetime.fromisocalendar(2020, 10, 1).astimezone()
    end_now = datetime.now(tz.tzlocal())
    
    assert type(start) == datetime
    assert start == begin

    assert type(end) == datetime
    assert end.isoformat()[0:18] == end_now.isoformat()[0:18]

def test_timespan_args_begin_iso_and_thru_str_missing():
    start, end = timespan(begin_str=ref_begin_iso)
    begin = datetime.fromisocalendar(2020, 10, 1).astimezone()
    end_now = datetime.now(tz.tzlocal())
    
    assert type(start) == datetime
    assert start == begin

    assert type(end) == datetime
    assert end.isoformat()[0:18] == end_now.isoformat()[0:18]

def test_timespan_args_begin_str_none_and_thru_iso():
    start, end = timespan(begin_str=None, thru_str=ref_thru_iso)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()
    thru = datetime.fromisocalendar(2020, 25, 1).astimezone() + timedelta(days=7)
    
    assert type(start) == datetime
    assert start.isoformat() == start_first.isoformat()

    assert type(end) == datetime
    assert end == thru

def test_timespan_args_begin_str_missing_and_thru_iso():
    start, end = timespan(thru_str=ref_thru_iso)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()
    thru = datetime.fromisocalendar(2020, 25, 1).astimezone() + timedelta(days=7)
    
    assert type(start) == datetime
    assert start.isoformat() == start_first.isoformat()

    assert type(end) == datetime
    assert end == thru

