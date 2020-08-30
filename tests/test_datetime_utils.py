"""Test functions for datetime_utils.py.
"""
from datetime import datetime, timedelta

from dateutil import tz

from squaredown.datetime_utils import timespan

# initialize module variables
REF_BEGIN_STR = '2020-06-01T00:00:00-04:00'
REF_THRU_STR = '2020-06-08T00:00:00-04:00'
REF_BEGIN_ISO = '2020-W10'
REF_THRU_ISO = '2020-W25'


def test_timespan_args_begin_str_and_thru_str():
    """Tests timespan arguments: begin_str, thru_str.
    """
    start, end = timespan(begin_str=REF_BEGIN_STR, thru_str=REF_THRU_STR)

    assert isinstance(start, datetime)
    assert start.isoformat() == REF_BEGIN_STR

    assert isinstance(end, datetime)
    assert end.isoformat() == REF_THRU_STR

def test_timespan_args_begin_str_and_thru_str_none():
    """Tests timespan arguments: begin_str, thru_str=None.
    """
    start, end = timespan(begin_str=REF_BEGIN_STR, thru_str=None)
    end_now = datetime.now(tz.tzlocal())

    assert isinstance(start, datetime)
    assert start.isoformat() == REF_BEGIN_STR

    assert isinstance(end, datetime)
    assert end.isoformat()[0:18] == end_now.isoformat()[0:18]

def test_timespan_args_begin_str_and_thru_str_missing():
    """Tests timespan arguments: begin_str, thru_str missing.
    """
    start, end = timespan(begin_str=REF_BEGIN_STR)
    end_now = datetime.now(tz.tzlocal())

    assert isinstance(start, datetime)
    assert start.isoformat() == REF_BEGIN_STR

    assert isinstance(end, datetime)
    assert end.isoformat()[0:18] == end_now.isoformat()[0:18]

def test_timespan_args_begin_str_none_and_thru_str():
    """Tests timespan arguments: begin_str=None, thru_str.
    """
    start, end = timespan(begin_str=None, thru_str=REF_THRU_STR)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()

    assert isinstance(start, datetime)
    assert start.isoformat() == start_first.isoformat()

    assert isinstance(end, datetime)
    assert end.isoformat() == REF_THRU_STR

def test_timespan_args_begin_str_missing_and_thru_str():
    """Tests timespan arguments, begin_str missing, thru_str.
    """
    start, end = timespan(thru_str=REF_THRU_STR)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()

    assert isinstance(start, datetime)
    assert start.isoformat() == start_first.isoformat()

    assert isinstance(end, datetime)
    assert end.isoformat() == REF_THRU_STR

def test_timespan_args_begin_and_thru():
    """Tests timespan arguments: begin, thru.
    """
    begin = datetime.fromisoformat(REF_BEGIN_STR)
    thru = datetime.fromisoformat(REF_THRU_STR)
    start, end = timespan(begin=begin, thru=thru)

    assert isinstance(start, datetime)
    assert start == begin

    assert isinstance(end, datetime)
    assert end == thru

def test_timespan_args_begin_and_thru_none():
    """Tests timespan arguments: begin, thru=None.
    """
    begin = datetime.fromisoformat(REF_BEGIN_STR)
    start, end = timespan(begin=begin, thru=None)
    thru_now = datetime.now(tz.tzlocal())

    assert isinstance(start, datetime)
    assert start == begin

    assert isinstance(end, datetime)
    assert end.isoformat()[0:18] == thru_now.isoformat()[0:18]

def test_timespan_args_begin_and_thru_missing():
    """Tests timespan arguments: begin, thru missing.
    """
    begin = datetime.fromisoformat(REF_BEGIN_STR)
    start, end = timespan(begin=begin)
    thru_now = datetime.now(tz.tzlocal())

    assert isinstance(start, datetime)
    assert start == begin

    assert isinstance(end, datetime)
    assert end.isoformat()[0:18] == thru_now.isoformat()[0:18]

def test_timespan_args_begin_none_and_thru():
    """Tests timespan arguments: begin=None, thru.
    """
    thru = datetime.fromisoformat(REF_THRU_STR)
    start, end = timespan(begin=None, thru=thru)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()

    assert isinstance(start, datetime)
    assert start == start_first

    assert isinstance(end, datetime)
    assert end == thru

def test_timespan_args_begin_missing_and_thru():
    """Tests timespan arguments: begin missing, thru.
    """
    thru = datetime.fromisoformat(REF_THRU_STR)
    start, end = timespan(thru=thru)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()

    assert isinstance(start, datetime)
    assert start == start_first

    assert isinstance(end, datetime)
    assert end == thru

def test_timespan_args_begin_iso_week_and_thru_iso_week():
    """Tests timespan arguments: begin iso week, thru iso week.
    """
    start, end = timespan(begin_str=REF_BEGIN_ISO, thru_str=REF_THRU_ISO)
    begin = datetime.fromisocalendar(2020, 10, 1).astimezone()
    thru = datetime.fromisocalendar(2020, 25, 1).astimezone() + timedelta(days=7)

    assert isinstance(start, datetime)
    assert start == begin

    assert isinstance(end, datetime)
    assert end == thru

def test_timespan_args_begin_iso_week_and_thru_str_none():
    """Tests timespan arguments: begin iso week, thru_str=None.
    """
    start, end = timespan(begin_str=REF_BEGIN_ISO, thru_str=None)
    begin = datetime.fromisocalendar(2020, 10, 1).astimezone()
    end_now = datetime.now(tz.tzlocal())

    assert isinstance(start, datetime)
    assert start == begin

    assert isinstance(end, datetime)
    assert end.isoformat()[0:18] == end_now.isoformat()[0:18]

def test_timespan_args_begin_iso_week_and_thru_str_missing():
    """Tests timespan arguments: begin iso week, thru_str missing.
    """
    start, end = timespan(begin_str=REF_BEGIN_ISO)
    begin = datetime.fromisocalendar(2020, 10, 1).astimezone()
    end_now = datetime.now(tz.tzlocal())

    assert isinstance(start, datetime)
    assert start == begin

    assert isinstance(end, datetime)
    assert end.isoformat()[0:18] == end_now.isoformat()[0:18]

def test_timespan_args_begin_str_none_and_thru_iso_week():
    """Tests timespan arguments: begin_str=None, thru iso week.
    """
    start, end = timespan(begin_str=None, thru_str=REF_THRU_ISO)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()
    thru = datetime.fromisocalendar(2020, 25, 1).astimezone() + timedelta(days=7)

    assert isinstance(start, datetime)
    assert start.isoformat() == start_first.isoformat()

    assert isinstance(end, datetime)
    assert end == thru

def test_timespan_args_begin_str_missing_and_thru_iso_week():
    """Tests timespan arguments: begin_str missing, thru iso week.
    """
    start, end = timespan(thru_str=REF_THRU_ISO)
    start_first = datetime(2000, 1, 1, 0, 0).astimezone()
    thru = datetime.fromisocalendar(2020, 25, 1).astimezone() + timedelta(days=7)

    assert isinstance(start, datetime)
    assert start.isoformat() == start_first.isoformat()

    assert isinstance(end, datetime)
    assert end == thru
