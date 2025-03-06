"""Test related to the pyarrow storage manager."""

import pytest
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from loguru import logger

from infrasys.arrow_storage import ArrowTimeSeriesStorage
from infrasys.in_memory_time_series_storage import InMemoryTimeSeriesStorage
from infrasys.system import System
from infrasys.time_series_models import (
    SingleTimeSeries,
    NonSequentialTimeSeries,
    TimeSeriesStorageType,
)

from .models.simple_system import SimpleSystem, SimpleBus, SimpleGenerator


@pytest.fixture(scope="session")
def test_system() -> System:
    system = SimpleSystem(time_series_directory=None)
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1)
    return system


def test_file_creation_with_single_time_series(test_system: System):
    gen1 = test_system.get_component(SimpleGenerator, "gen1")
    ts = SingleTimeSeries.from_array(
        data=range(8784),
        variable_name="active_power",
        initial_time=datetime(year=2020, month=1, day=1),
        resolution=timedelta(hours=1),
    )
    test_system.time_series.add(ts, gen1, scenario="one", model_year="2030")
    time_series = test_system.time_series.get(gen1)
    assert isinstance(test_system.time_series.storage, ArrowTimeSeriesStorage)
    base_directory = test_system.get_time_series_directory()
    assert isinstance(base_directory, Path)
    time_series_fpath = base_directory.joinpath(str(time_series.uuid) + ".arrow")
    assert time_series_fpath.exists()


def test_file_creation_with_nonsequential_time_series(test_system: System):
    gen1 = test_system.get_component(SimpleGenerator, "gen1")
    timestamps = [
        datetime(year=2030, month=1, day=1) + timedelta(seconds=5 * i) for i in range(10)
    ]
    ts = NonSequentialTimeSeries.from_array(
        data=range(10),
        timestamps=timestamps,
        variable_name="active_power",
    )
    test_system.time_series.add(ts, gen1, scenario="one", model_year="2030")
    time_series = test_system.time_series.get(gen1, time_series_type=NonSequentialTimeSeries)
    assert isinstance(test_system.time_series.storage, ArrowTimeSeriesStorage)
    base_directory = test_system.get_time_series_directory()
    assert isinstance(base_directory, Path)
    time_series_fpath = base_directory.joinpath(str(time_series.uuid) + ".arrow")
    assert time_series_fpath.exists()


def test_copy_files_with_single_time_series(tmp_path):
    """Test that we can copy the time series from tmp to specified folder"""
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1)
    ts = SingleTimeSeries.from_array(
        data=range(8784),
        variable_name="active_power",
        initial_time=datetime(year=2020, month=1, day=1),
        resolution=timedelta(hours=1),
    )
    system.time_series.add(ts, gen1, scenario="two", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)

    logger.info("Starting deserialization")
    system2 = SimpleSystem.from_json(filename)
    gen1b = system2.get_component(SimpleGenerator, gen1.name)
    time_series = system2.time_series.get(gen1b)
    time_series_fpath = (
        tmp_path / system2.get_time_series_directory() / (str(time_series.uuid) + ".arrow")
    )

    assert time_series_fpath.exists()


def test_copy_files_with_nonsequential_timeseries(tmp_path):
    """Test that we can copy the time series from tmp to specified folder"""
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1)
    timestamps = [
        datetime(year=2030, month=1, day=1) + timedelta(seconds=5 * i) for i in range(10)
    ]
    ts = NonSequentialTimeSeries.from_array(
        data=range(10),
        timestamps=timestamps,
        variable_name="active_power",
    )
    system.time_series.add(ts, gen1, scenario="two", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)

    logger.info("Starting deserialization")
    system2 = SimpleSystem.from_json(filename)
    gen1b = system2.get_component(SimpleGenerator, gen1.name)
    time_series = system2.time_series.get(gen1b, time_series_type=NonSequentialTimeSeries)
    time_series_fpath = (
        tmp_path / system2.get_time_series_directory() / (str(time_series.uuid) + ".arrow")
    )

    assert time_series_fpath.exists()


def test_read_deserialize_single_time_series(tmp_path):
    """Test that we can read from a deserialized system."""
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1)
    ts = SingleTimeSeries.from_array(
        data=range(8784),
        variable_name="active_power",
        initial_time=datetime(year=2020, month=1, day=1),
        resolution=timedelta(hours=1),
    )
    system.time_series.add(ts, gen1, scenario="high", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)

    system2 = SimpleSystem.from_json(filename, time_series_directory=tmp_path)
    gen1b = system2.get_component(SimpleGenerator, gen1.name)
    deserialize_ts = system2.time_series.get(gen1b)
    assert isinstance(deserialize_ts, SingleTimeSeries)
    assert deserialize_ts.resolution == ts.resolution
    assert deserialize_ts.initial_time == ts.initial_time
    assert isinstance(deserialize_ts.data, np.ndarray)
    length = ts.length
    assert isinstance(length, int)
    assert np.array_equal(deserialize_ts.data, np.array(range(length)))


def test_read_deserialize_nonsequential_time_series(tmp_path):
    """Test that we can read from a deserialized system."""
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1)
    timestamps = [
        datetime(year=2030, month=1, day=1) + timedelta(seconds=5 * i) for i in range(10)
    ]
    ts = NonSequentialTimeSeries.from_array(
        data=range(10),
        timestamps=timestamps,
        variable_name="active_power",
    )
    system.time_series.add(ts, gen1, scenario="high", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)

    system2 = SimpleSystem.from_json(filename, time_series_directory=tmp_path)
    gen1b = system2.get_component(SimpleGenerator, gen1.name)
    deserialize_ts = system2.time_series.get(gen1b, time_series_type=NonSequentialTimeSeries)
    assert isinstance(deserialize_ts, NonSequentialTimeSeries)
    assert isinstance(deserialize_ts.data, np.ndarray)
    assert isinstance(deserialize_ts.timestamps, np.ndarray)
    length = ts.length
    assert isinstance(length, int)
    assert np.array_equal(deserialize_ts.data, np.array(range(length)))
    assert np.array_equal(deserialize_ts.timestamps, np.array(timestamps))


def test_copied_storage_system_single_time_series(simple_system_with_time_series):
    assert isinstance(
        simple_system_with_time_series._time_series_mgr._storage, ArrowTimeSeriesStorage
    )
    gen_component = next(simple_system_with_time_series.get_components(SimpleGenerator))
    data_array_1 = simple_system_with_time_series.list_time_series(gen_component)[0].data

    simple_system_with_time_series.convert_storage(
        time_series_storage_type=TimeSeriesStorageType.MEMORY
    )

    assert isinstance(
        simple_system_with_time_series._time_series_mgr._storage,
        InMemoryTimeSeriesStorage,
    )

    data_array_2 = simple_system_with_time_series.list_time_series(gen_component)[0].data
    assert np.array_equal(data_array_1, data_array_2)


def test_copied_storage_system_nonsequential_time_series(
    simple_system_with_nonsequential_time_series,
):
    assert isinstance(
        simple_system_with_nonsequential_time_series._time_series_mgr._storage,
        ArrowTimeSeriesStorage,
    )
    gen_component = next(
        simple_system_with_nonsequential_time_series.get_components(SimpleGenerator)
    )

    data_array_1 = simple_system_with_nonsequential_time_series.list_time_series(
        gen_component, time_series_type=NonSequentialTimeSeries
    )[0].data
    timestamps_array_1 = simple_system_with_nonsequential_time_series.list_time_series(
        gen_component, time_series_type=NonSequentialTimeSeries
    )[0].timestamps

    simple_system_with_nonsequential_time_series.convert_storage(
        time_series_type=NonSequentialTimeSeries,
        time_series_storage_type=TimeSeriesStorageType.MEMORY,
    )

    assert isinstance(
        simple_system_with_nonsequential_time_series._time_series_mgr._storage,
        InMemoryTimeSeriesStorage,
    )

    data_array_2 = simple_system_with_nonsequential_time_series.list_time_series(
        gen_component, time_series_type=NonSequentialTimeSeries
    )[0].data
    timestamps_array_2 = simple_system_with_nonsequential_time_series.list_time_series(
        gen_component, time_series_type=NonSequentialTimeSeries
    )[0].timestamps
    assert np.array_equal(data_array_1, data_array_2)
    assert np.array_equal(timestamps_array_1, timestamps_array_2)
