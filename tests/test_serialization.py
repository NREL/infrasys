import random
from datetime import datetime, timedelta

import numpy as np
import pyarrow as pa
import pytest

from infrasys.location import Location
from infrasys.component_models import ComponentWithQuantities
from infrasys.quantities import Distance, ActivePower
from infrasys.time_series_models import SingleTimeSeries
from .models.simple_system import (
    SimpleSystem,
    SimpleBus,
    SimpleGenerator,
    SimpleSubsystem,
)


class ComponentWithPintQuantity(ComponentWithQuantities):
    """Test component with a container of quantities."""

    distance: Distance


def test_serialization(tmp_path):
    system = SimpleSystem(name="test-system", my_attr=5)
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
        system.components.add(geo, bus, gen1, gen2, subsystem)

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
