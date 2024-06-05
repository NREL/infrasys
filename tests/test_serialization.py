import json
import random
import os
from datetime import datetime, timedelta

import numpy as np
import pyarrow as pa
import pytest
from pydantic import WithJsonSchema
from typing_extensions import Annotated

from infrasys import Location, SingleTimeSeries
from infrasys.component import Component
from infrasys.quantities import Distance, ActivePower
from infrasys.exceptions import ISOperationNotAllowed
from infrasys.normalization import NormalizationMax
from .models.simple_system import (
    SimpleSystem,
    SimpleBus,
    SimpleGenerator,
    SimpleSubsystem,
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
        if key not in ("_component_mgr", "_time_series_mgr"):
            assert getattr(system2, key) == val

    components2 = list(system2.iter_all_components())
    assert len(components2) == num_components

    for component in components:
        component2 = system2.get_component_by_uuid(component.uuid)
        for key, val in component.__dict__.items():
            assert getattr(component2, key) == val


@pytest.mark.parametrize("time_series_in_memory", [True, False])
def test_serialize_time_series(tmp_path, time_series_in_memory):
    system = SimpleSystem(time_series_in_memory=time_series_in_memory)
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = SimpleGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1, gen2)

    variable_name = "active_power"
    length = 8784
    df = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(df, variable_name, start, resolution)
    system.add_time_series(ts, gen1, gen2, scenario="high", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)

    system2 = SimpleSystem.from_json(filename, time_series_read_only=True)
    system2_ts_dir = system2.get_time_series_directory()
    assert system2_ts_dir is not None
    assert system2_ts_dir == SimpleSystem._make_time_series_directory(filename)
    gen1b = system.get_component(SimpleGenerator, gen1.name)
    with pytest.raises(ISOperationNotAllowed):
        system2.remove_time_series(gen1b, variable_name=variable_name)

    ts2 = system.get_time_series(gen1b, variable_name=variable_name)
    assert ts2.data == ts.data

    system3 = SimpleSystem.from_json(filename, time_series_read_only=False)
    assert system3.get_time_series_directory() != SimpleSystem._make_time_series_directory(
        filename
    )
    system3_ts_dir = system3.get_time_series_directory()
    assert system3_ts_dir is not None
    assert not system3_ts_dir.is_relative_to(system2_ts_dir)


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
    system.add_components(gen.bus.coordinates, gen.bus, gen, component)
    sys_file = tmp_path / "system.json"
    system.to_json(sys_file)
    system2 = SimpleSystem.from_json(sys_file)
    c1 = system.get_component(ComponentWithPintQuantity, "test")
    c2 = system2.get_component(ComponentWithPintQuantity, "test")
    if isinstance(c1.distance.magnitude, np.ndarray):
        assert (c2.distance == c1.distance).all()
    else:
        assert c2.distance == c1.distance


def test_with_time_series_quantity(tmp_path):
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
    ts2 = system2.get_time_series(gen2, variable_name=variable_name)
    assert isinstance(ts, SingleTimeSeries)
    assert ts.length == length
    assert ts.resolution == resolution
    assert ts.initial_time == initial_time
    assert isinstance(ts2.data.magnitude, pa.Array)
    assert ts2.data[-1].as_py() == length - 1
    assert ts2.data.magnitude == pa.array(range(length))


@pytest.mark.parametrize("in_memory", [True, False])
def test_system_with_time_series_normalization(tmp_path, in_memory):
    system = SimpleSystem(
        name="test-system", auto_add_composed_components=True, time_series_in_memory=in_memory
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
    ts2 = system2.get_time_series(gen2, variable_name=variable_name)
    assert ts2.normalization.max_value == length - 1


def test_json_schema():
    schema = ComponentWithPintQuantity.model_json_schema()
    assert isinstance(json.loads(json.dumps(schema)), dict)


def test_system_save(tmp_path, simple_system_with_time_series):
    simple_system = simple_system_with_time_series
    custom_folder = "my_system"
    fpath = tmp_path / custom_folder
    fname = "test_system"
    simple_system.save(fpath, filename=fname)
    assert os.path.exists(fpath), f"Folder {fpath} was not created successfully"
    assert os.path.exists(fpath / fname), f"Serialized system {fname} was not created successfully"

    fname = "test_system"
    with pytest.raises(FileExistsError):
        simple_system.save(fpath, filename=fname)

    fname = "test_system"
    simple_system.save(fpath, filename=fname, overwrite=True)
    assert os.path.exists(fpath), f"Folder {fpath} was not created successfully"
    assert os.path.exists(fpath / fname), f"Serialized system {fname} was not created successfully"

    custom_folder = "my_system_zip"
    fpath = tmp_path / custom_folder
    simple_system.save(fpath, filename=fname, zip=True)
    assert not os.path.exists(fpath), f"Original folder {fpath} was not deleted sucessfully."
    zip_fpath = f"{fpath}.zip"
    assert os.path.exists(zip_fpath), f"Zip file {zip_fpath} does not exists"
