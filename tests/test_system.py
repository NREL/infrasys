import itertools
from datetime import timedelta, datetime
from uuid import uuid4

import pytest

from infrasys.exceptions import (
    ISAlreadyAttached,
    ISNotStored,
    ISOperationNotAllowed,
    ISConflictingArguments,
)
from infrasys.location import Location
from infrasys.component_models import Component
from infrasys.quantities import ActivePower
from infrasys.time_series_models import SingleTimeSeries
from .models.simple_system import (
    GeneratorBase,
    SimpleSystem,
    SimpleBus,
    SimpleGenerator,
    SimpleSubsystem,
    RenewableGenerator,
)


def test_system():
    system = SimpleSystem()
    geo = Location(x=1.0, y=2.0)
    bus = SimpleBus(name="test-bus", voltage=1.1, coordinates=geo)
    gen = SimpleGenerator(name="test-gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    subsystem = SimpleSubsystem(name="test-subsystem", generators=[gen])
    system.add_components(geo, bus, gen, subsystem)

    gen2 = system.get_component(SimpleGenerator, "test-gen")
    assert gen2 is gen
    assert gen2.bus is bus

    with pytest.raises(ISNotStored):
        system.get_component(SimpleGenerator, "not-present")


def test_system_auto_add_composed_components():
    system = SimpleSystem(auto_add_composed_components=False)
    assert not system.auto_add_composed_components
    geo = Location(x=1.0, y=2.0)
    bus = SimpleBus(name="test-bus", voltage=1.1, coordinates=geo)

    with pytest.raises(ISOperationNotAllowed):
        system.add_component(bus)

    system.auto_add_composed_components = True
    assert system.auto_add_composed_components
    system.add_component(bus)
    assert len(list(system.get_components(Location))) == 1


def test_system_auto_add_composed_components_list():
    system = SimpleSystem(auto_add_composed_components=False)
    assert not system.auto_add_composed_components
    subsystem = SimpleSubsystem.example()
    with pytest.raises(ISOperationNotAllowed):
        system.add_component(subsystem)
    system.auto_add_composed_components = True
    assert system.auto_add_composed_components
    system.add_component(subsystem)


def test_get_components(simple_system: SimpleSystem):
    system = simple_system
    initial_count = 4
    assert len(list(system.get_components(Component))) == initial_count
    system.auto_add_composed_components = True
    for _ in range(5):
        gen = RenewableGenerator.example()
        system.add_component(gen)
    all_components = list(system.get_components(Component))
    # 5 generators, each includes a bus and location
    assert len(all_components) == initial_count + 5 * 3
    generators = list(
        system.get_components(GeneratorBase, filter_func=lambda x: x.name == "renewable-gen")
    )
    assert len(generators) == 5

    with pytest.raises(ISOperationNotAllowed):
        system.get_component(RenewableGenerator, "renewable-gen")

    assert len(list(system.list_components_by_name(RenewableGenerator, "renewable-gen"))) == 5

    with pytest.raises(ISNotStored):
        system.get_component_by_uuid(uuid4())

    stored_types = sorted((x.__name__ for x in system.get_component_types()))
    assert stored_types == [
        "Location",
        "RenewableGenerator",
        "SimpleBus",
        "SimpleGenerator",
        "SimpleSubsystem",
    ]


def test_get_components_multiple_types():
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = SimpleGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen3 = RenewableGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1, gen2, gen3)

    selected_components = list(system.get_components(SimpleBus, SimpleGenerator))
    assert len(selected_components) == 3  # 2 SimpleGenerator + 1 SimpleBus

    # Validate that filter_function works as well
    selected_components = list(
        system.get_components(
            SimpleGenerator, RenewableGenerator, filter_func=lambda x: x.name == "gen2"
        )
    )
    assert len(selected_components) == 2  # 1 SimpleGenerator + 1 RenewableGenerator


def test_time_series_attach_from_array():
    system = SimpleSystem()
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
    system.add_time_series(ts, gen1, gen2)
    assert gen1.has_time_series(variable_name=variable_name)
    assert gen2.has_time_series(variable_name=variable_name)
    assert system.get_time_series(gen1, variable_name=variable_name).data == ts.data


def test_time_series():
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = SimpleGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1, gen2)

    length = 10
    initial_time = datetime(year=2020, month=1, day=1)
    time_array = [initial_time + timedelta(hours=i) for i in range(length)]
    data = range(length)
    variable_name = "active_power"
    ts = SingleTimeSeries.from_time_array(data, variable_name, time_array)
    system.add_time_series(ts, gen1, gen2)
    assert gen1.has_time_series(variable_name=variable_name)
    assert gen2.has_time_series(variable_name=variable_name)
    assert system.get_time_series(gen1, variable_name=variable_name) == ts
    system.remove_time_series(gen1, gen2, variable_name=variable_name)
    with pytest.raises(ISNotStored):
        system.get_time_series(gen1, variable_name=variable_name)

    assert not gen1.has_time_series(variable_name=variable_name)
    assert not gen2.has_time_series(variable_name=variable_name)


@pytest.mark.parametrize("params", list(itertools.product([True, False], [True, False])))
def test_time_series_retrieval(params):
    in_memory, use_quantity = params
    system = SimpleSystem(time_series_in_memory=in_memory)
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen = SimpleGenerator(name="gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen)

    length = 10
    initial_time = datetime(year=2020, month=1, day=1)
    time_array = [initial_time + timedelta(hours=i) for i in range(length)]
    iterable = range(length)
    data = ActivePower(iterable, "watts") if use_quantity else iterable
    variable_name = "active_power"
    ts = SingleTimeSeries.from_time_array(data, variable_name, time_array)
    system.add_time_series(ts, gen, scenario="high", model_year="2030")
    system.add_time_series(ts, gen, scenario="high", model_year="2035")
    system.add_time_series(ts, gen, scenario="low", model_year="2030")
    system.add_time_series(ts, gen, scenario="low", model_year="2035")

    with pytest.raises(ISAlreadyAttached):
        system.add_time_series(ts, gen, scenario="low", model_year="2035")

    assert gen.has_time_series(variable_name=variable_name)
    assert gen.has_time_series(variable_name=variable_name, scenario="high")
    assert gen.has_time_series(variable_name=variable_name, scenario="high", model_year="2030")
    assert not gen.has_time_series(variable_name=variable_name, model_year="2036")
    assert (
        system.get_time_series(
            gen, variable_name=variable_name, scenario="high", model_year="2030"
        )
        == ts
    )
    with pytest.raises(ISOperationNotAllowed):
        system.get_time_series(gen, variable_name=variable_name, scenario="high")
    with pytest.raises(ISNotStored):
        system.get_time_series(gen, variable_name=variable_name, scenario="medium")
    assert len(system.list_time_series(gen, variable_name=variable_name, scenario="high")) == 2
    assert len(system.list_time_series(gen, variable_name=variable_name)) == 4
    system.remove_time_series(gen, variable_name=variable_name, scenario="high")
    assert len(system.list_time_series(gen, variable_name=variable_name)) == 2
    system.remove_time_series(gen, variable_name=variable_name)
    assert not gen.has_time_series(variable_name=variable_name)


def test_time_series_removal():
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = SimpleGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_components(bus, gen1, gen2)

    variable_names = ["active_power", "reactive_power"]
    uuids = []
    for variable_name in variable_names:
        length = 8784
        data = range(length)
        start = datetime(year=2020, month=1, day=1)
        resolution = timedelta(hours=1)
        ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
        uuids.append(ts.uuid)
        for gen in (gen1, gen2):
            system.add_time_series(ts, gen, scenario="high", model_year="2030")
            system.add_time_series(ts, gen, scenario="high", model_year="2035")
            system.add_time_series(ts, gen, scenario="low", model_year="2030")
            system.add_time_series(ts, gen, scenario="low", model_year="2035")

    system.remove_time_series(gen1, variable_name="active_power")
    system.remove_time_series(gen1, variable_name="reactive_power")
    assert not system.list_time_series(gen1, variable_name="active_power")
    assert not system.list_time_series(gen1, variable_name="reactive_power")
    assert system.list_time_series(gen2, variable_name="active_power")
    assert system.list_time_series(gen2, variable_name="reactive_power")
    system.remove_time_series(gen2)
    assert not system.list_time_series(gen2, variable_name="active_power")
    assert not system.list_time_series(gen2, variable_name="reactive_power")


def test_time_series_read_only():
    system = SimpleSystem(time_series_read_only=True)
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen = SimpleGenerator(name="gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.add_component(bus)
    system.add_component(gen)

    variable_name = "active_power"
    length = 8784
    data = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    with pytest.raises(ISOperationNotAllowed):
        system.add_time_series(ts, gen)


def test_serialize_time_series_from_array(tmp_path):
    system = SimpleSystem()
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

    system2 = SimpleSystem.from_json(filename, time_series_read_only=True)
    gen1b = system2.components.get(SimpleGenerator, gen1.name)
    with pytest.raises(ISOperationNotAllowed):
        system2.remove_time_series(gen1b, variable_name=variable_name)
    ts2 = system.get_time_series(gen1b, variable_name=variable_name)
    assert ts2.data.tolist() == list(data)


@pytest.mark.parametrize("in_memory", [True, False])
def test_time_series_slices(in_memory):
    system = SimpleSystem(
        name="test-system", auto_add_composed_components=True, time_series_in_memory=in_memory
    )
    gen = SimpleGenerator.example()
    system.components.add(gen)
    variable_name = "active_power"
    length = 8784
    data = list(range(length))
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    system.add_time_series(ts, gen)

    first_timestamp = start
    second_timestamp = start + resolution
    last_timestamp = start + (length - 1) * resolution
    assert len(system.time_series.get(gen, variable_name=variable_name).data) == length
    assert len(system.time_series.get(gen, variable_name=variable_name, length=10).data) == 10
    ts2 = system.time_series.get(
        gen, variable_name=variable_name, start_time=second_timestamp, length=5
    )
    assert len(ts2.data) == 5
    assert ts2.data.tolist() == data[1:6]

    assert (
        len(
            system.time_series.get(
                gen, variable_name=variable_name, start_time=second_timestamp
            ).data
        )
        == len(data) - 1
    )

    with pytest.raises(ISConflictingArguments, match="is less than"):
        system.time_series.get(
            gen,
            variable_name=variable_name,
            start_time=first_timestamp - ts.resolution,
            length=5,
        )
    with pytest.raises(ISConflictingArguments, match="is too large"):
        system.time_series.get(
            gen,
            variable_name=variable_name,
            start_time=last_timestamp + ts.resolution,
            length=5,
        )
    with pytest.raises(ISConflictingArguments, match="conflicts with initial_time"):
        system.time_series.get(
            gen,
            variable_name=variable_name,
            start_time=first_timestamp + timedelta(minutes=1),
        )
    with pytest.raises(ISConflictingArguments, match=r"start_time.*length.*conflicts with"):
        system.time_series.get(
            gen,
            variable_name=variable_name,
            start_time=second_timestamp,
            length=len(data),
        )


def test_copy_component(simple_system_with_time_series: SimpleSystem):
    system = simple_system_with_time_series
    gen1 = system.get_component(SimpleGenerator, "test-gen")

    gen2 = system.copy_component(gen1)
    assert gen2.uuid != gen1.uuid
    assert gen2.name == gen1.name
    assert gen2.system_uuid is None

    gen3 = system.copy_component(gen1, name="gen3")
    assert gen3.name == "gen3"
    assert gen2.system_uuid is None

    gen4 = system.copy_component(gen1, name="gen4", attach=True)
    assert gen4.name == "gen4"
    assert gen4.system_uuid == gen1.system_uuid


@pytest.mark.parametrize("in_memory", [True, False])
def test_remove_component(in_memory):
    system = SimpleSystem(
        name="test-system", auto_add_composed_components=True, time_series_in_memory=in_memory
    )
    gen1 = SimpleGenerator.example()
    system.components.add(gen1)
    gen2 = system.copy_component(gen1, name="gen2", attach=True)
    variable_name = "active_power"
    length = 8784
    data = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    system.add_time_series(ts, gen1, gen2)

    with pytest.raises(ISOperationNotAllowed):
        system.components.remove(gen1)

    system.remove_component_by_name(type(gen1), gen1.name)
    assert not gen1.has_time_series()
    assert gen2.has_time_series()

    system.remove_component_by_uuid(gen2.uuid)
    assert not gen2.has_time_series()
    assert gen2.system_uuid is None

    with pytest.raises(ISNotStored):
        system.remove_component(gen2)

    with pytest.raises(ISNotStored):
        system.components.remove(gen2)

    for gen in (gen1, gen2):
        with pytest.raises(ISNotStored):
            system.components.get(SimpleGenerator, gen.name)
