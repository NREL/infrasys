from infrasys.chronify_time_series_storage import ChronifyTimeSeriesStorage
from .models.simple_system import SimpleSystem, SimpleBus, SimpleGenerator
from infrasys.time_series_models import (
    SingleTimeSeries,
    NonSequentialTimeSeries,
    TimeSeriesStorageType,
)
from infrasys.exceptions import ISAlreadyAttached
from infrasys.arrow_storage import ArrowTimeSeriesStorage
from infrasys.in_memory_time_series_storage import InMemoryTimeSeriesStorage
from datetime import timedelta, datetime
import numpy as np
import pytest


@pytest.mark.parametrize(
    "original_kwargs,new_kwargs,original_stype,new_stype",
    [
        (
            {"time_series_storage_type": TimeSeriesStorageType.MEMORY},
            {},
            InMemoryTimeSeriesStorage,
            ArrowTimeSeriesStorage,
        ),
        (
            {"time_series_storage_type": TimeSeriesStorageType.MEMORY},
            {"time_series_storage_type": TimeSeriesStorageType.CHRONIFY},
            InMemoryTimeSeriesStorage,
            ChronifyTimeSeriesStorage,
        ),
        (
            {"time_series_storage_type": TimeSeriesStorageType.CHRONIFY},
            {"time_series_storage_type": TimeSeriesStorageType.ARROW},
            ChronifyTimeSeriesStorage,
            ArrowTimeSeriesStorage,
        ),
        (
            {"time_series_storage_type": TimeSeriesStorageType.ARROW},
            {"time_series_storage_type": TimeSeriesStorageType.CHRONIFY},
            ArrowTimeSeriesStorage,
            ChronifyTimeSeriesStorage,
        ),
        (
            {},
            {"time_series_storage_type": TimeSeriesStorageType.MEMORY},
            ArrowTimeSeriesStorage,
            InMemoryTimeSeriesStorage,
        ),
    ],
)
def test_convert_storage_single_time_series(
    original_kwargs, new_kwargs, original_stype, new_stype
):
    test_bus = SimpleBus.example()
    test_generator = SimpleGenerator.example()
    system = SimpleSystem(auto_add_composed_components=True, **original_kwargs)

    assert isinstance(system._time_series_mgr._storage, original_stype)

    system.get_components()
    system.add_components(test_bus)
    system.add_components(test_generator)

    test_time_series_data = SingleTimeSeries(
        data=np.arange(24),
        resolution=timedelta(hours=1),
        initial_time=datetime(2020, 1, 1),
        variable_name="load",
    )
    system.add_time_series(test_time_series_data, test_generator)
    with pytest.raises(ISAlreadyAttached):
        system.add_time_series(test_time_series_data, test_generator)

    system.convert_storage(**new_kwargs)

    assert isinstance(system._time_series_mgr._storage, new_stype)

    ts2 = system.get_time_series(
        test_generator, time_series_type=SingleTimeSeries, variable_name="load"
    )
    assert np.array_equal(ts2.data_array, test_time_series_data.data_array)


@pytest.mark.parametrize(
    "original_kwargs,new_kwargs,original_stype,new_stype",
    [
        (
            {"time_series_storage_type": TimeSeriesStorageType.MEMORY},
            {},
            InMemoryTimeSeriesStorage,
            ArrowTimeSeriesStorage,
        ),
        (
            {},
            {"time_series_storage_type": TimeSeriesStorageType.MEMORY},
            ArrowTimeSeriesStorage,
            InMemoryTimeSeriesStorage,
        ),
    ],
)
def test_convert_storage_nonsequential_time_series(
    original_kwargs, new_kwargs, original_stype, new_stype
):
    test_bus = SimpleBus.example()
    test_generator = SimpleGenerator.example()
    system = SimpleSystem(auto_add_composed_components=True, **original_kwargs)

    assert isinstance(system._time_series_mgr._storage, original_stype)

    system.get_components()
    system.add_components(test_bus)
    system.add_components(test_generator)

    timestamps = np.array(
        [datetime(year=2030, month=1, day=1) + timedelta(seconds=5 * i) for i in range(24)],
    )
    test_time_series_data = NonSequentialTimeSeries(
        data=np.arange(24),
        timestamps=timestamps,
        variable_name="load",
    )
    system.add_time_series(test_time_series_data, test_generator)
    with pytest.raises(ISAlreadyAttached):
        system.add_time_series(test_time_series_data, test_generator)
    system.convert_storage(**new_kwargs)

    assert isinstance(system._time_series_mgr._storage, new_stype)
    ts2 = system.get_time_series(
        test_generator, time_series_type=NonSequentialTimeSeries, variable_name="load"
    )
    assert np.array_equal(ts2.data_array, test_time_series_data.data_array)
    assert np.array_equal(ts2.timestamps, test_time_series_data.timestamps)
