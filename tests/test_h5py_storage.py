from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from infrasys import System
from infrasys.exceptions import ISAlreadyAttached
from infrasys.h5_time_series_storage import HDF5TimeSeriesStorage
from infrasys.time_series_models import SingleTimeSeries, TimeSeriesStorageType
from infrasys.time_series_storage_base import TimeSeriesStorageBase
from tests.models.simple_system import SimpleBus, SimpleGenerator


@pytest.fixture(scope="function")
def system_with_h5_storage(tmp_path):
    storage_type = TimeSeriesStorageType.HDF5
    return System(
        name="TestSystem",
        time_series_storage_type=storage_type,
        time_series_directory=tmp_path,
        auto_add_composed_components=True,
        in_memory=True,
    )


def test_initialize_h5_storage(tmp_path):
    h5_storage = HDF5TimeSeriesStorage(directory=tmp_path)
    assert isinstance(h5_storage, TimeSeriesStorageBase)


def test_missing_module(missing_modules, tmp_path):
    storage_type = TimeSeriesStorageType.HDF5
    with missing_modules("h5py"):
        with pytest.raises(ImportError):
            _ = System(
                name="test", time_series_storage_type=storage_type, time_series_directory=tmp_path
            )


def test_storage_initialization(tmp_path):
    storage_type = TimeSeriesStorageType.HDF5
    system = System(
        name="test", time_series_storage_type=storage_type, time_series_directory=tmp_path
    )
    assert isinstance(system._time_series_mgr.storage, HDF5TimeSeriesStorage)


def test_handler_creation(tmp_path):
    storage_type = TimeSeriesStorageType.HDF5
    system = System(
        name="test",
        time_series_storage_type=storage_type,
        time_series_directory=tmp_path,
        auto_add_composed_components=True,
    )
    storage = system._time_series_mgr.storage
    assert isinstance(storage, HDF5TimeSeriesStorage)


def test_h5_time_series(tmp_path):
    storage_type = TimeSeriesStorageType.HDF5
    system = System(
        name="test",
        time_series_storage_type=storage_type,
        time_series_directory=tmp_path,
        auto_add_composed_components=True,
    )

    # Adding some example components
    bus = SimpleBus(name="test", voltage=1.1)
    gen = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)

    system.add_component(gen)

    ts = SingleTimeSeries.from_array(
        data=range(8784),
        name="active_power",
        initial_timestamp=datetime(year=2020, month=1, day=1),
        resolution=timedelta(hours=1),
    )
    system.add_time_series(ts, gen, scenario="one", model_year="2030")
    time_series = system.get_time_series(gen)
    assert np.array_equal(time_series.data, ts.data)

    system.remove_time_series(gen)

    assert not system.has_time_series(gen)


def test_h5py_serialization(tmp_path, system_with_h5_storage):
    system = system_with_h5_storage

    # Adding some example components
    bus = SimpleBus(name="test", voltage=1.1)
    gen = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)

    system.add_component(gen)

    ts = SingleTimeSeries.from_array(
        data=range(8784),
        name="active_power",
        initial_timestamp=datetime(year=2020, month=1, day=1),
        resolution=timedelta(hours=1),
    )
    system.add_time_series(ts, gen, scenario="one", model_year="2030")

    # Serialize
    fpath = tmp_path / Path("test.json")
    system.to_json(fpath)
    fname = system._time_series_mgr.storage.STORAGE_FILE
    output_time_series_file = tmp_path / f"{fpath.stem}_time_series" / fname
    assert (output_time_series_file).exists()

    # Deserialize
    system_deserialized = System.from_json(fpath)
    storage_deserialized = system_deserialized._time_series_mgr.storage
    assert isinstance(storage_deserialized, HDF5TimeSeriesStorage)
    gen2 = system.get_component(SimpleGenerator, name="gen1")
    time_series = system_deserialized.get_time_series(gen2)
    assert np.array_equal(time_series.data, ts.data)


def test_h5_context_manager(system_with_h5_storage):
    system = system_with_h5_storage

    bus = SimpleBus(name="test", voltage=1.1)
    gen = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)

    system.add_component(gen)

    ts_name = "test_ts"
    ts = SingleTimeSeries.from_array(
        data=range(8784),
        name=ts_name,
        initial_timestamp=datetime(year=2020, month=1, day=1),
        resolution=timedelta(hours=1),
    )
    with pytest.raises(ISAlreadyAttached):
        with system.open_time_series_store(mode="a"):
            system.add_time_series(ts, gen, scenario="one", model_year="2030")
            system.add_time_series(ts, gen, scenario="one", model_year="2030")

    # Not a single time series should have been added.
    assert not system.has_time_series(gen, name=ts_name)
