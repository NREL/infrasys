import json
from pathlib import Path
import random
import os
from datetime import datetime, timedelta
from typing import Type

import numpy as np
from numpy._typing import NDArray
import pint
import pytest
from pydantic import WithJsonSchema
from typing_extensions import Annotated

from infrasys import Location, SingleTimeSeries, NonSequentialTimeSeries, System
from infrasys.component import Component
from infrasys.quantities import Distance, ActivePower
from infrasys.exceptions import ISOperationNotAllowed
from infrasys.normalization import NormalizationMax
from infrasys.time_series_models import TimeSeriesStorageType, TimeSeriesData
from .models.simple_system import (
    SimpleSystem,
    SimpleBus,
    SimpleGenerator,
    SimpleSubsystem,
)

TS_STORAGE_OPTIONS = (
    TimeSeriesStorageType.ARROW,
    TimeSeriesStorageType.CHRONIFY,
    TimeSeriesStorageType.MEMORY,
)

# chronify not yet implemented for nonsequentialtimeseries
TS_STORAGE_OPTIONS_NONSEQUENTIAL = (
    TimeSeriesStorageType.ARROW,
    TimeSeriesStorageType.MEMORY,
)


class ComponentWithPintQuantity(Component):
    """Test component with a container of quantities."""

    distance: Annotated[Distance, WithJsonSchema({"type": "string"})]


def test_serialization(tmp_path):
    system = SimpleSystem(name="test-system", description="a test system", my_attr=5)
    num_components_by_type = 5
    for i in range(num_components_by_type):
        geo = Location(x=random.random(), y=random.random())
        bus = SimpleBus(name=f"test-bus{i}", voltage=random.random(), coordinates=geo)
        gen1 = SimpleGenerator(
            name=f"test-gen{i}a",
            active_power=random.random(),
            rating=random.random(),
            bus=bus,
            available=True,
        )
        gen2 = SimpleGenerator(
            name=f"test-gen{i}b",
            active_power=random.random(),
            rating=random.random(),
            bus=bus,
            available=True,
        )
        subsystem = SimpleSubsystem(name="test-subsystem", generators=[gen1, gen2])
        system.add_components(geo, bus, gen1, gen2, subsystem)

    components = list(system.iter_all_components())
    num_components = len(components)
    assert num_components == num_components_by_type * (1 + 1 + 2 + 1)

    filename = tmp_path / "system.json"
    system.to_json(filename, overwrite=True)
    system2 = SimpleSystem.from_json(filename)
    for key, val in system.__dict__.items():
        if key not in (
            "_component_mgr",
            "_supplemental_attr_mgr",
            "_time_series_mgr",
            "_con",
        ):
            assert getattr(system2, key) == val

    components2 = list(system2.iter_all_components())
    assert len(components2) == num_components

    for component in components:
        component2 = system2.get_component_by_uuid(component.uuid)
        for key, val in component.__dict__.items():
            assert getattr(component2, key) == val


@pytest.mark.parametrize("time_series_storage_type", TS_STORAGE_OPTIONS)
def test_serialize_single_time_series(tmp_path, time_series_storage_type):
    system = SimpleSystem(time_series_storage_type=time_series_storage_type)
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = SimpleGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1, gen2)

    variable_name = "active_power"
    length = 8784
    data = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    system.add_time_series(ts, gen1, gen2, scenario="high", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)
    system2 = check_deserialize_with_read_write_time_series(filename)
    gen1b = system2.get_component(SimpleGenerator, gen1.name)
    gen2b = system2.get_component(SimpleGenerator, gen2.name)
    data2 = range(1, length + 1)
    ts2 = SingleTimeSeries.from_array(data2, variable_name, start, resolution)
    system2.add_time_series(ts2, gen1b, gen2b, scenario="low", model_year="2030")
    filename2 = tmp_path / "system2.json"
    system2.to_json(filename2)
    system3 = SimpleSystem.from_json(filename2)
    assert np.array_equal(
        system3.get_time_series(
            gen1b,
            time_series_type=SingleTimeSeries,
            variable_name=variable_name,
            scenario="low",
            model_year="2030",
        ).data,
        data2,
    )
    assert np.array_equal(
        system3.get_time_series(
            gen2b,
            time_series_type=SingleTimeSeries,
            variable_name=variable_name,
            scenario="low",
            model_year="2030",
        ).data,
        data2,
    )
    check_deserialize_with_read_only_time_series(
        filename, gen1.name, gen2.name, variable_name, ts.data
    )


def check_deserialize_with_read_only_time_series(
    filename,
    gen1_name: str,
    gen2_name: str,
    variable_name: str,
    expected_ts_data: NDArray | pint.Quantity,
    expected_ts_timestamps: NDArray | None = None,
    time_series_type: Type[TimeSeriesData] = SingleTimeSeries,
):
    system = SimpleSystem.from_json(filename, time_series_read_only=True)
    system_ts_dir = system.get_time_series_directory()
    assert system_ts_dir is not None
    assert system_ts_dir == SimpleSystem._make_time_series_directory(filename)
    gen1b = system.get_component(SimpleGenerator, gen1_name)
    with pytest.raises(ISOperationNotAllowed):
        system.remove_time_series(gen1b, variable_name=variable_name)

    ts2 = system.get_time_series(
        gen1b, time_series_type=time_series_type, variable_name=variable_name
    )
    assert np.array_equal(ts2.data, expected_ts_data)
    if expected_ts_timestamps is not None:
        assert np.array_equal(ts2.timestamps, expected_ts_timestamps)


@pytest.mark.parametrize("time_series_storage_type", TS_STORAGE_OPTIONS_NONSEQUENTIAL)
def test_serialize_nonsequential_time_series(tmp_path, time_series_storage_type):
    "Test serialization of NonSequentialTimeSeries"
    system = SimpleSystem(time_series_storage_type=time_series_storage_type)
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = SimpleGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1, gen2)

    variable_name = "active_power"
    length = 10
    data = range(length)
    timestamps = [
        datetime(year=2030, month=1, day=1) + timedelta(seconds=5 * i) for i in range(length)
    ]
    ts = NonSequentialTimeSeries.from_array(
        data=data, variable_name=variable_name, timestamps=timestamps
    )
    system.add_time_series(ts, gen1, gen2, scenario="high", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)

    check_deserialize_with_read_write_time_series(filename)
    check_deserialize_with_read_only_time_series(
        filename,
        gen1.name,
        gen2.name,
        variable_name,
        ts.data,
        ts.timestamps,
        time_series_type=NonSequentialTimeSeries,
    )


def check_deserialize_with_read_write_time_series(filename) -> System:
    system3 = SimpleSystem.from_json(filename, time_series_read_only=False)
    assert system3.get_time_series_directory() != SimpleSystem._make_time_series_directory(
        filename
    )
    system3_ts_dir = system3.get_time_series_directory()
    assert system3_ts_dir is not None
    return system3


@pytest.mark.parametrize(
    "distance",
    [
        Distance(2, "meter"),
        Distance([2, 3], "meter"),
        Distance([[2, 3, 4], [5, 6, 7]], "meter"),
    ],
)
def test_serialize_quantity(tmp_path, distance):
    system = SimpleSystem()
    gen = SimpleGenerator.example()
    component = ComponentWithPintQuantity(name="test", distance=distance)
    assert gen.bus.coordinates is not None
    system.add_components(gen.bus.coordinates, gen.bus, gen, component)
    sys_file = tmp_path / "system.json"
    system.to_json(sys_file)
    system2 = SimpleSystem.from_json(sys_file)
    c1 = system.get_component(ComponentWithPintQuantity, "test")
    c2 = system2.get_component(ComponentWithPintQuantity, "test")
    if isinstance(c1.distance.magnitude, np.ndarray):
        assert (c2.distance == c1.distance).all()  # type: ignore
    else:
        assert c2.distance == c1.distance


def test_with_single_time_series_quantity(tmp_path):
    """Test serialization of SingleTimeSeries with a Pint quantity."""
    system = SimpleSystem(auto_add_composed_components=True)
    gen = SimpleGenerator.example()
    system.add_components(gen)
    length = 10
    initial_time = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    data = ActivePower(range(length), "watts")
    variable_name = "active_power"
    ts = SingleTimeSeries.from_array(data, variable_name, initial_time, resolution)
    system.add_time_series(ts, gen)

    sys_file = tmp_path / "system.json"
    system.to_json(sys_file)

    system2 = SimpleSystem.from_json(sys_file)
    gen2 = system2.get_component(SimpleGenerator, gen.name)
    ts2 = system2.get_time_series(
        gen2, time_series_type=SingleTimeSeries, variable_name=variable_name
    )
    assert isinstance(ts, SingleTimeSeries)
    assert ts.length == length
    assert ts.resolution == resolution
    assert ts.initial_time == initial_time
    assert isinstance(ts2.data.magnitude, np.ndarray)
    assert np.array_equal(ts2.data.magnitude, np.array(range(length)))


def test_with_nonsequential_time_series_quantity(tmp_path):
    """Test serialization of SingleTimeSeries with a Pint quantity."""
    system = SimpleSystem(auto_add_composed_components=True)
    gen = SimpleGenerator.example()
    system.add_components(gen)
    length = 10
    data = ActivePower(range(length), "watts")
    variable_name = "active_power"
    timestamps = [
        datetime(year=2030, month=1, day=1) + timedelta(seconds=100 * i) for i in range(10)
    ]
    ts = NonSequentialTimeSeries.from_array(
        data=data, variable_name=variable_name, timestamps=timestamps
    )
    system.add_time_series(ts, gen)

    sys_file = tmp_path / "system.json"
    system.to_json(sys_file)

    system2 = SimpleSystem.from_json(sys_file)
    gen2 = system2.get_component(SimpleGenerator, gen.name)
    ts2 = system2.get_time_series(
        gen2, time_series_type=NonSequentialTimeSeries, variable_name=variable_name
    )
    assert isinstance(ts, NonSequentialTimeSeries)
    assert ts.length == length
    assert isinstance(ts2.data.magnitude, np.ndarray)
    assert isinstance(ts2.timestamps, np.ndarray)
    assert np.array_equal(ts2.data.magnitude, np.array(range(length)))
    assert np.array_equal(ts2.timestamps, np.array(timestamps))


@pytest.mark.parametrize("storage_type", TS_STORAGE_OPTIONS)
def test_system_with_single_time_series_normalization(tmp_path, storage_type):
    system = SimpleSystem(
        name="test-system",
        auto_add_composed_components=True,
        time_series_storage_type=storage_type,
    )
    gen = SimpleGenerator.example()
    system.add_components(gen)
    variable_name = "active_power"
    length = 8784
    data = list(range(length))
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(
        data, variable_name, start, resolution, normalization=NormalizationMax()
    )
    system.add_time_series(ts, gen)
    filename = tmp_path / "sys.json"
    system.to_json(filename)

    system2 = SimpleSystem.from_json(filename)
    gen2 = system2.get_component(SimpleGenerator, gen.name)
    ts2 = system2.get_time_series(
        gen2, time_series_type=SingleTimeSeries, variable_name=variable_name
    )
    assert ts2.normalization.max_value == length - 1


def test_json_schema():
    schema = ComponentWithPintQuantity.model_json_schema()
    assert isinstance(json.loads(json.dumps(schema)), dict)


def test_system_save(tmp_path, simple_system_with_time_series):
    simple_system = simple_system_with_time_series
    custom_folder = "my_system"
    fpath = tmp_path / custom_folder
    fname = "test_system.json"
    simple_system.save(fpath, filename=fname)
    assert os.path.exists(fpath), f"Folder {fpath} was not created successfully"
    assert os.path.exists(fpath / fname), f"Serialized system {fname} was not created successfully"

    with pytest.raises(FileExistsError):
        simple_system.save(fpath, filename=fname)

    simple_system.save(fpath, filename=fname, overwrite=True)
    assert os.path.exists(fpath), f"Folder {fpath} was not created successfully"
    assert os.path.exists(fpath / fname), f"Serialized system {fname} was not created successfully"

    custom_folder = "my_system_zip"
    fpath = tmp_path / custom_folder
    simple_system.save(fpath, filename=fname, zip=True)
    assert not os.path.exists(fpath), f"Original folder {fpath} was not deleted sucessfully."
    zip_fpath = f"{fpath}.zip"
    assert os.path.exists(zip_fpath), f"Zip file {zip_fpath} does not exists"


def test_legacy_format():
    # This file was save from v0.2.1 with test_with_time_series_quantity.
    # Ensure that we can deserialize it.
    SimpleSystem.from_json(Path("tests/data/legacy_system.json"))


def test_convert_chronify_storage_permanent(tmp_path):
    gen = SimpleGenerator.example()
    system = SimpleSystem(
        auto_add_composed_components=True, time_series_storage_type=TimeSeriesStorageType.ARROW
    )
    system.add_components(gen)
    variable_name = "active_power"
    length = 10
    data = list(range(length))
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    system.add_time_series(ts, gen)
    system.convert_storage(
        time_series_storage_type=TimeSeriesStorageType.CHRONIFY,
        time_series_directory=tmp_path,
        in_place=False,
        permanent=True,
    )
    assert (tmp_path / "time_series_data.db").exists()
