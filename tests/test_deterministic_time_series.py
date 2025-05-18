from datetime import datetime, timedelta

import numpy as np
import pytest

from infrasys.quantities import ActivePower
from infrasys.time_series_models import (
    DeterministicSingleTimeSeries,
    DeterministicTimeSeries,
    SingleTimeSeries,
    TimeSeriesStorageType,
)
from tests.models.simple_system import SimpleGenerator, SimpleSystem


TS_STORAGE_OPTIONS = (
    TimeSeriesStorageType.ARROW,
    TimeSeriesStorageType.HDF5,
)


@pytest.mark.parametrize("storage_type", TS_STORAGE_OPTIONS)
def test_with_deterministic_time_series_quantity(tmp_path, storage_type):
    """Test serialization of DeterministicTimeSeries with a Pint quantity and different storage types."""
    system = SimpleSystem(auto_add_composed_components=True, time_series_storage_type=storage_type)
    gen = SimpleGenerator.example()
    system.add_components(gen)

    initial_time = datetime(year=2020, month=9, day=1)
    resolution = timedelta(hours=1)
    horizon = timedelta(hours=8)
    interval = timedelta(hours=1)
    window_count = 3

    forecast_data = [
        [100.0, 101.0, 101.3, 90.0, 98.0, 87.0, 88.0, 67.0],
        [101.0, 101.3, 99.0, 98.0, 88.9, 88.3, 67.1, 89.4],
        [99.0, 67.0, 89.0, 99.9, 100.0, 101.0, 112.0, 101.3],
    ]

    data = ActivePower(np.array(forecast_data), "watts")
    name = "active_power_forecast"
    ts = DeterministicTimeSeries.from_array(
        data, name, initial_time, resolution, horizon, interval, window_count
    )
    system.add_time_series(ts, gen)

    sys_file = tmp_path / "system.json"
    system.to_json(sys_file)

    system2 = SimpleSystem.from_json(sys_file)
    gen2 = system2.get_component(SimpleGenerator, gen.name)
    ts2 = system2.get_time_series(gen2, name=name)
    assert isinstance(ts, DeterministicTimeSeries)
    assert ts2.resolution == resolution
    assert ts2.initial_timestamp == initial_time


@pytest.mark.parametrize("storage_type", TS_STORAGE_OPTIONS)
def test_with_deterministic_single_time_series_quantity(tmp_path, storage_type):
    """Test serialization of DeterministicSingleTimeSeries with a Pint quantity and different storage types."""
    system = SimpleSystem(auto_add_composed_components=True, time_series_storage_type=storage_type)
    gen = SimpleGenerator.example()
    system.add_components(gen)

    initial_timestamp = datetime(year=2020, month=1, day=1)
    name = "active_power"
    ts = SingleTimeSeries.from_array(
        data=range(8784),
        name=name,
        resolution=timedelta(hours=1),
        initial_timestamp=initial_timestamp,
    )
    horizon = timedelta(hours=8)
    interval = timedelta(hours=1)
    ts_deterministic = DeterministicSingleTimeSeries.from_single_time_series(
        ts, interval=interval, horizon=horizon
    )
    system.add_time_series(ts_deterministic, gen)

    sys_file = tmp_path / "system.json"
    system.to_json(sys_file)

    system2 = SimpleSystem.from_json(sys_file)
    gen2 = system2.get_component(SimpleGenerator, gen.name)
    ts2 = system2.get_time_series(gen2, name=name, time_series_type=DeterministicSingleTimeSeries)
    assert isinstance(ts_deterministic, DeterministicSingleTimeSeries)
    assert ts2.horizon == horizon
    assert ts2.initial_timestamp == initial_timestamp
