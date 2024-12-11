from .models.simple_system import SimpleSystem, SimpleBus, SimpleGenerator
from infrasys.time_series_models import SingleTimeSeries
from infrasys.exceptions import ISAlreadyAttached
from datetime import timedelta, datetime
import numpy as np
import pytest


def get_data_and_uuids(system):
    uuids = system._time_series_mgr.metadata_store.unique_uuids_by_type("SingleTimeSeries")
    data = {
        uuid: system._time_series_mgr._storage._get_raw_single_time_series(uuid) for uuid in uuids
    }
    return uuids, data


@pytest.mark.parametrize(
    "original_kwargs,new_kwargs",
    [
        ({"time_series_in_memory": True}, {}),
        ({}, {"time_series_in_memory": True}),
    ],
)
def test_memory_convert_storage_time_series(original_kwargs, new_kwargs):
    test_bus = SimpleBus.example()
    test_generator = SimpleGenerator.example()
    system = SimpleSystem(auto_add_composed_components=True, **original_kwargs)
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

    original_uuids, original_data = get_data_and_uuids(system)

    system.convert_storage(**new_kwargs)

    new_uuids, new_data = get_data_and_uuids(system)

    assert set(original_uuids) == set(new_uuids)

    for uuid in new_uuids:
        assert np.array_equal(original_data[uuid], new_data[uuid])
