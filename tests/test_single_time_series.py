from datetime import datetime, timedelta
import polars as pl

import pytest

from infrasys.time_series_models import TIME_COLUMN, SingleTimeSeries, VALUE_COLUMN


def test_from_array():
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    length = 8784
    data = range(length)
    variable_name = "active_power"
    ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    assert ts.length == length
    assert ts.resolution == resolution
    assert isinstance(ts.data, pl.DataFrame)
    assert ts.data[VALUE_COLUMN][-1] == length - 1


def test_from_dataframe(hourly_time_array):
    df = hourly_time_array
    variable_name = VALUE_COLUMN
    ts = SingleTimeSeries.from_dataframe(df, variable_name)
    assert ts.length == len(df)
    assert ts.resolution == timedelta(hours=1)
    assert isinstance(ts.data, pl.DataFrame)
    assert ts.data[VALUE_COLUMN][-1] == len(df) - 1


def test_invalid_length():
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    length = 1
    data = range(length)
    variable_name = "active_power"
    with pytest.raises(ValueError, match="length must be at least 2"):
        SingleTimeSeries.from_array(data, variable_name, start, resolution)


def test_invalid_time_column(hourly_time_array):
    df = hourly_time_array.select(pl.col(TIME_COLUMN).alias("invalid"), pl.col(VALUE_COLUMN))
    variable_name = "value"
    with pytest.raises(ValueError, match="must have the time column"):
        SingleTimeSeries(data=df, variable_name=variable_name)


def test_invalid_value_column(hourly_time_array):
    df = hourly_time_array.select(pl.col(TIME_COLUMN), pl.col(VALUE_COLUMN).alias("invalid"))
    variable_name = VALUE_COLUMN
    with pytest.raises(ValueError, match="must have the value column"):
        SingleTimeSeries(data=df, variable_name=variable_name)


def test_inconsistent_resolution(hourly_time_array):
    df = hourly_time_array
    variable_name = VALUE_COLUMN
    with pytest.raises(ValueError, match="does not match data resolution"):
        SingleTimeSeries(data=df, variable_name=variable_name, resolution=timedelta(seconds=10))


def test_inconsistent_length(hourly_time_array):
    df = hourly_time_array
    variable_name = VALUE_COLUMN
    with pytest.raises(ValueError, match="does not match data length"):
        SingleTimeSeries(data=df, variable_name=variable_name, length=10)


def test_inconsistent_initial_time(hourly_time_array):
    df = hourly_time_array
    variable_name = VALUE_COLUMN
    with pytest.raises(ValueError, match="does not match data initial_time"):
        SingleTimeSeries(data=df, variable_name=variable_name, initial_time=datetime.now())
