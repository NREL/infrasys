from .models.simple_system import SimpleSystem, SimpleBus, SimpleGenerator
from infrasys.time_series_models import SingleTimeSeries, NonSequentialTimeSeries
from infrasys.exceptions import ISAlreadyAttached
from infrasys.arrow_storage import ArrowTimeSeriesStorage
from infrasys.in_memory_time_series_storage import InMemoryTimeSeriesStorage
from datetime import timedelta, datetime
import numpy as np
import pytest


def get_data_and_uuids_single_time_series(system):
    uuids = system._time_series_mgr.metadata_store.unique_uuids_by_type(SingleTimeSeries.__name__)
    data = {uuid: system._time_series_mgr._storage.get_raw_time_series(uuid) for uuid in uuids}
    return uuids, data


def get_data_and_uuids_nonsequential_time_series(system):
    uuids = system._time_series_mgr.metadata_store.unique_uuids_by_type(
        NonSequentialTimeSeries.__name__
    )
    data = {uuid: system._time_series_mgr._storage.get_raw_time_series(uuid) for uuid in uuids}
    return uuids, data


@pytest.mark.parametrize(
    "original_kwargs,new_kwargs,original_stype,new_stype",
    [
        ({"time_series_in_memory": True}, {}, InMemoryTimeSeriesStorage, ArrowTimeSeriesStorage),
        ({}, {"time_series_in_memory": True}, ArrowTimeSeriesStorage, InMemoryTimeSeriesStorage),
    ],
)
def test_memory_convert_storage_single_time_series(
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
        initial_time=datetime.now(),
        variable_name="load",
    )
    system.add_time_series(test_time_series_data, test_generator)
    with pytest.raises(ISAlreadyAttached):
        system.add_time_series(test_time_series_data, test_generator)

    original_uuids, original_data = get_data_and_uuids_single_time_series(system)

    system.convert_storage(time_series_type=SingleTimeSeries, **new_kwargs)

    assert isinstance(system._time_series_mgr._storage, new_stype)
    new_uuids, new_data = get_data_and_uuids_single_time_series(system)

    assert set(original_uuids) == set(new_uuids)

    for uuid in new_uuids:
        assert np.array_equal(original_data[uuid], new_data[uuid])


@pytest.mark.parametrize(
    "original_kwargs,new_kwargs,original_stype,new_stype",
    [
        ({"time_series_in_memory": True}, {}, InMemoryTimeSeriesStorage, ArrowTimeSeriesStorage),
        ({}, {"time_series_in_memory": True}, ArrowTimeSeriesStorage, InMemoryTimeSeriesStorage),
    ],
)
def test_memory_convert_storage_nonsequential_time_series(
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

    original_uuids, original_data = get_data_and_uuids_nonsequential_time_series(system)
    system.convert_storage(time_series_type=NonSequentialTimeSeries, **new_kwargs)

    assert isinstance(system._time_series_mgr._storage, new_stype)
    new_uuids, new_data = get_data_and_uuids_nonsequential_time_series(system)

    assert set(original_uuids) == set(new_uuids)
    for uuid in new_uuids:
        assert np.array_equal(original_data[uuid], new_data[uuid])
