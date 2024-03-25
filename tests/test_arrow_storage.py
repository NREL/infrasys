"""Test related to the pyarrow storage manager."""
import pytest
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
from loguru import logger

from infrasys.arrow_storage import ArrowTimeSeriesStorage
from infrasys.system import System
from infrasys.time_series_models import SingleTimeSeries

from .models.simple_system import SimpleSystem, SimpleBus, SimpleGenerator


@pytest.fixture(scope="session")
def test_system() -> System:
    system = SimpleSystem(time_series_directory=None)
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1)
    return system


def test_file_creation(test_system: System):
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


def test_copy_files(tmp_path):
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
    system2 = SimpleSystem.from_json(filename, base_directory=tmp_path)
    gen1b = system2.get_component(SimpleGenerator, gen1.name)
    time_series = system2.time_series.get(gen1b)
    time_series_fpath = (
        tmp_path / system2.get_time_series_directory() / (str(time_series.uuid) + ".arrow")
    )

    assert time_series_fpath.exists()


def test_read_deserialize_time_series(tmp_path):
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
    assert isinstance(deserialize_ts.data, pa.Array)
    assert deserialize_ts.data[-1].as_py() == ts.length - 1
