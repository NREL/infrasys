"""Tests for DeterministicSingleTimeSeriesMetadata - zero-copy deterministic views."""

from datetime import datetime, timedelta

import numpy as np
import pytest

from infrasys.quantities import ActivePower
from infrasys.time_series_models import (
    Deterministic,
    DeterministicMetadata,
    SingleTimeSeries,
    TimeSeriesStorageType,
)
from tests.models.simple_system import SimpleGenerator, SimpleSystem

TS_STORAGE_OPTIONS = (
    TimeSeriesStorageType.ARROW,
    TimeSeriesStorageType.HDF5,
    TimeSeriesStorageType.MEMORY,
)


@pytest.mark.parametrize("storage_type", TS_STORAGE_OPTIONS)
def test_add_deterministic_single_time_series(tmp_path, storage_type):
    """Test adding a DeterministicSingleTimeSeries view without copying data."""
    system = SimpleSystem(auto_add_composed_components=True, time_series_storage_type=storage_type)
    gen = SimpleGenerator.example()
    system.add_components(gen)

    initial_timestamp = datetime(year=2020, month=1, day=1)
    data = np.array(range(100))
    name = "active_power"
    resolution = timedelta(hours=1)

    ts = SingleTimeSeries.from_array(
        data=data,
        name=name,
        resolution=resolution,
        initial_timestamp=initial_timestamp,
    )
    system.add_time_series(ts, gen)

    horizon = timedelta(hours=8)
    interval = timedelta(hours=1)
    window_count = 5

    _ = system.time_series.add_deterministic_single_time_series(
        owner=gen,
        single_time_series_name=name,
        interval=interval,
        horizon=horizon,
        window_count=window_count,
    )

    ts_det = system.get_time_series(gen, name=name, time_series_type=Deterministic)

    assert isinstance(ts_det, Deterministic)
    assert ts_det.window_count == window_count
    assert ts_det.horizon == horizon
    assert ts_det.interval == interval
    assert ts_det.resolution == resolution
    assert ts_det.initial_timestamp == initial_timestamp

    horizon_steps = int(horizon / resolution)
    interval_steps = int(interval / resolution)

    for window_idx in range(window_count):
        start_idx = window_idx * interval_steps
        end_idx = start_idx + horizon_steps
        expected_window = data[start_idx:end_idx]
        np.testing.assert_array_equal(ts_det.data[window_idx, :], expected_window)


@pytest.mark.parametrize("storage_type", TS_STORAGE_OPTIONS)
def test_deterministic_single_time_series_with_quantity(tmp_path, storage_type):
    """Test DeterministicSingleTimeSeries with Pint quantities."""
    system = SimpleSystem(auto_add_composed_components=True, time_series_storage_type=storage_type)
    gen = SimpleGenerator.example()
    system.add_components(gen)

    initial_timestamp = datetime(year=2020, month=1, day=1)
    data = ActivePower(np.array(range(100)), "watts")
    name = "active_power"
    resolution = timedelta(hours=1)

    ts = SingleTimeSeries.from_array(
        data=data,
        name=name,
        resolution=resolution,
        initial_timestamp=initial_timestamp,
    )
    system.add_time_series(ts, gen)

    horizon = timedelta(hours=8)
    interval = timedelta(hours=4)

    system.time_series.add_deterministic_single_time_series(
        owner=gen,
        single_time_series_name=name,
        interval=interval,
        horizon=horizon,
    )

    ts_det = system.get_time_series(gen, name=name, time_series_type=Deterministic)

    assert isinstance(ts_det, Deterministic)
    assert isinstance(ts_det.data, ActivePower)
    assert ts_det.data.units == data.units


@pytest.mark.parametrize("storage_type", TS_STORAGE_OPTIONS)
def test_deterministic_single_time_series_serialization(tmp_path, storage_type):
    """Test that DeterministicSingleTimeSeries survives serialization/deserialization."""
    system = SimpleSystem(auto_add_composed_components=True, time_series_storage_type=storage_type)
    gen = SimpleGenerator.example()
    system.add_components(gen)

    initial_timestamp = datetime(year=2020, month=1, day=1)
    data = np.array(range(100))
    name = "active_power"
    resolution = timedelta(hours=1)

    ts = SingleTimeSeries.from_array(
        data=data,
        name=name,
        resolution=resolution,
        initial_timestamp=initial_timestamp,
    )
    system.add_time_series(ts, gen)

    horizon = timedelta(hours=8)
    interval = timedelta(hours=1)
    window_count = 10

    system.time_series.add_deterministic_single_time_series(
        owner=gen,
        single_time_series_name=name,
        interval=interval,
        horizon=horizon,
        window_count=window_count,
    )

    sys_file = tmp_path / "system.json"
    system.to_json(sys_file)

    system2 = SimpleSystem.from_json(sys_file)
    gen2 = system2.get_component(SimpleGenerator, gen.name)

    ts_det = system2.get_time_series(gen2, name=name, time_series_type=Deterministic)

    assert isinstance(ts_det, Deterministic)
    assert ts_det.window_count == window_count
    assert ts_det.horizon == horizon
    assert ts_det.interval == interval

    horizon_steps = int(horizon / resolution)
    interval_steps = int(interval / resolution)

    for window_idx in range(window_count):
        start_idx = window_idx * interval_steps
        end_idx = start_idx + horizon_steps
        expected_window = data[start_idx:end_idx]
        np.testing.assert_array_equal(ts_det.data[window_idx, :], expected_window)


def test_deterministic_single_time_series_metadata_creation():
    """Test creating DeterministicMetadata directly from SingleTimeSeries."""
    initial_timestamp = datetime(year=2020, month=1, day=1)
    data = np.array(range(100))
    name = "test_ts"
    resolution = timedelta(hours=1)

    ts = SingleTimeSeries.from_array(
        data=data,
        name=name,
        resolution=resolution,
        initial_timestamp=initial_timestamp,
    )

    horizon = timedelta(hours=8)
    interval = timedelta(hours=4)
    window_count = 5

    metadata = DeterministicMetadata.from_single_time_series(
        ts,
        interval=interval,
        horizon=horizon,
        window_count=window_count,
    )

    assert metadata.name == name
    assert metadata.time_series_uuid == ts.uuid  # Points to the SingleTimeSeries
    assert metadata.initial_timestamp == initial_timestamp
    assert metadata.resolution == resolution
    assert metadata.interval == interval
    assert metadata.horizon == horizon
    assert metadata.window_count == window_count
    assert metadata.type == "Deterministic"


def test_deterministic_single_time_series_metadata_auto_window_count():
    """Test that window_count is calculated automatically when not provided."""
    initial_timestamp = datetime(year=2020, month=1, day=1)
    data = np.array(range(100))
    name = "test_ts"
    resolution = timedelta(hours=1)

    ts = SingleTimeSeries.from_array(
        data=data,
        name=name,
        resolution=resolution,
        initial_timestamp=initial_timestamp,
    )

    horizon = timedelta(hours=8)
    interval = timedelta(hours=4)

    metadata = DeterministicMetadata.from_single_time_series(
        ts,
        interval=interval,
        horizon=horizon,
    )

    assert metadata.window_count == 24


def test_deterministic_single_time_series_metadata_insufficient_data():
    """Test error when SingleTimeSeries is too short for the requested parameters."""
    initial_timestamp = datetime(year=2020, month=1, day=1)
    data = np.array(range(10))
    name = "test_ts"
    resolution = timedelta(hours=1)

    ts = SingleTimeSeries.from_array(
        data=data,
        name=name,
        resolution=resolution,
        initial_timestamp=initial_timestamp,
    )

    horizon = timedelta(hours=8)
    interval = timedelta(hours=4)
    window_count = 5

    with pytest.raises(ValueError, match="insufficient"):
        DeterministicMetadata.from_single_time_series(
            ts,
            interval=interval,
            horizon=horizon,
            window_count=window_count,
        )


def test_deterministic_single_time_series_metadata_get_range():
    """Test the get_range method of DeterministicMetadata from SingleTimeSeries."""
    initial_timestamp = datetime(year=2020, month=1, day=1)
    data = np.array(range(100))
    name = "test_ts"
    resolution = timedelta(hours=1)

    ts = SingleTimeSeries.from_array(
        data=data,
        name=name,
        resolution=resolution,
        initial_timestamp=initial_timestamp,
    )

    horizon = timedelta(hours=8)
    interval = timedelta(hours=4)
    window_count = 5

    metadata = DeterministicMetadata.from_single_time_series(
        ts,
        interval=interval,
        horizon=horizon,
        window_count=window_count,
    )

    index, length = metadata.get_range()
    assert index == 0
    horizon_steps = int(horizon / resolution)
    interval_steps = int(interval / resolution)
    expected_length = interval_steps * (window_count - 1) + horizon_steps
    assert length == expected_length

    start_time = initial_timestamp + timedelta(hours=4)
    index, length = metadata.get_range(start_time=start_time)
    assert index == 4
    assert length == expected_length - 4


@pytest.mark.parametrize("storage_type", TS_STORAGE_OPTIONS)
def test_multiple_deterministic_views_from_same_single_ts(tmp_path, storage_type):
    """Test creating multiple deterministic views from the same SingleTimeSeries."""
    system = SimpleSystem(auto_add_composed_components=True, time_series_storage_type=storage_type)
    gen = SimpleGenerator.example()
    system.add_components(gen)

    initial_timestamp = datetime(year=2020, month=1, day=1)
    data = np.array(range(200))
    name = "active_power"
    resolution = timedelta(hours=1)

    ts = SingleTimeSeries.from_array(
        data=data,
        name=name,
        resolution=resolution,
        initial_timestamp=initial_timestamp,
    )
    system.add_time_series(ts, gen)

    system.time_series.add_deterministic_single_time_series(
        owner=gen,
        single_time_series_name=name,
        interval=timedelta(hours=1),
        horizon=timedelta(hours=6),
        window_count=10,
        forecast_type="short_term",
    )

    system.time_series.add_deterministic_single_time_series(
        owner=gen,
        single_time_series_name=name,
        interval=timedelta(hours=24),
        horizon=timedelta(hours=48),
        window_count=5,
        forecast_type="long_term",
    )

    ts_short = system.get_time_series(
        gen, name=name, time_series_type=Deterministic, forecast_type="short_term"
    )
    ts_long = system.get_time_series(
        gen, name=name, time_series_type=Deterministic, forecast_type="long_term"
    )

    assert ts_short.horizon == timedelta(hours=6)
    assert ts_short.interval == timedelta(hours=1)
    assert ts_short.window_count == 10

    assert ts_long.horizon == timedelta(hours=48)
    assert ts_long.interval == timedelta(hours=24)
    assert ts_long.window_count == 5
