from datetime import timedelta, datetime
from uuid import uuid4

import pytest

from infrasys.exceptions import (
    ISAlreadyAttached,
    ISNotStored,
    ISOperationNotAllowed,
)
from infrasys.location import Location
from infrasys.component_models import Component
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
    system.components.add(geo, bus, gen, subsystem)

    gen2 = system.components.get(SimpleGenerator, "test-gen")
    assert gen2 is gen
    assert gen2.bus is bus

    with pytest.raises(ISNotStored):
        system.components.get(SimpleGenerator, "not-present")


def test_serialization(tmp_path, simple_system):
    system = simple_system
    custom_attr = 10
    system.my_attr = custom_attr
    geos = list(system.components.iter(Location))
    assert len(geos) == 1
    geo = geos[0]
    bus = system.components.get(SimpleBus, "test-bus")
    gen = system.components.get(SimpleGenerator, "test-gen")
    subsystem = system.components.get(SimpleSubsystem, "test-subsystem")

    filename = tmp_path / "system.json"
    system.to_json(filename, overwrite=True, indent=2)
    sys2 = SimpleSystem.from_json(filename)
    assert sys2.components.get_by_uuid(geo.uuid) == geo
    assert sys2.components.get(SimpleBus, "test-bus") == bus
    assert sys2.components.get(SimpleGenerator, "test-gen") == gen
    assert sys2.components.get(SimpleSubsystem, "test-subsystem") == subsystem
    assert sys2.my_attr == custom_attr


def test_get_components(simple_system):
    system = simple_system
    for _ in range(5):
        gen = RenewableGenerator.example()
        system.add_component(gen)
    all_components = list(system.get_components(Component))
    assert len(all_components) == 9
    generators = list(
        system.get_components(GeneratorBase, filter_func=lambda x: x.name == "renewable-gen")
    )
    assert len(generators) == 5

    with pytest.raises(ISOperationNotAllowed):
        system.get_component(RenewableGenerator, "renewable-gen")

    assert len(list(system.list_components_by_name(RenewableGenerator, "renewable-gen"))) == 5

    with pytest.raises(ISNotStored):
        system.components.get_by_uuid(uuid4())


def test_time_series_attach_from_array():
    system = SimpleSystem()
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
    system.time_series.add(ts, gen1, gen2)
    assert gen1.has_time_series(variable_name=variable_name)
    assert gen2.has_time_series(variable_name=variable_name)
    assert system.time_series.get(gen1, variable_name=variable_name).data == ts.data


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
    system.time_series.add(ts, gen1, gen2)
    assert gen1.has_time_series(variable_name=variable_name)
    assert gen2.has_time_series(variable_name=variable_name)
    assert system.time_series.get(gen1, variable_name=variable_name) == ts
    system.time_series.remove(gen1, gen2, variable_name=variable_name)
    with pytest.raises(ISNotStored):
        system.time_series.get(gen1, variable_name=variable_name)

    assert not gen1.has_time_series(variable_name=variable_name)
    assert not gen2.has_time_series(variable_name=variable_name)


def test_time_series_retrieval():
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen = SimpleGenerator(name="gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.components.add(bus, gen)

    length = 10
    initial_time = datetime(year=2020, month=1, day=1)
    time_array = [initial_time + timedelta(hours=i) for i in range(length)]
    data = range(length)
    variable_name = "active_power"
    ts = SingleTimeSeries.from_time_array(data, variable_name, time_array)
    system.time_series.add(ts, gen, scenario="high", model_year="2030")
    system.time_series.add(ts, gen, scenario="high", model_year="2035")
    system.time_series.add(ts, gen, scenario="low", model_year="2030")
    system.time_series.add(ts, gen, scenario="low", model_year="2035")

    with pytest.raises(ISAlreadyAttached):
        system.time_series.add(ts, gen, scenario="low", model_year="2035")

    assert gen.has_time_series(variable_name=variable_name)
    assert gen.has_time_series(variable_name=variable_name, scenario="high")
    assert gen.has_time_series(variable_name=variable_name, scenario="high", model_year="2030")
    assert not gen.has_time_series(variable_name=variable_name, model_year="2036")
    assert (
        system.time_series.get(
            gen, variable_name=variable_name, scenario="high", model_year="2030"
        )
        == ts
    )
    with pytest.raises(ISOperationNotAllowed):
        system.time_series.get(gen, variable_name=variable_name, scenario="high")
    with pytest.raises(ISNotStored):
        system.time_series.get(gen, variable_name=variable_name, scenario="medium")
    assert (
        len(system.time_series.list_time_series(gen, variable_name=variable_name, scenario="high"))
        == 2
    )
    assert len(system.time_series.list_time_series(gen, variable_name=variable_name)) == 4
    system.time_series.remove(gen, variable_name=variable_name, scenario="high")
    assert len(system.time_series.list_time_series(gen, variable_name=variable_name)) == 2
    system.time_series.remove(gen, variable_name=variable_name)
    assert not gen.has_time_series(variable_name=variable_name)


def test_time_series_removal():
    system = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = SimpleGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.components.add(bus, gen1, gen2)

    variable_names = ["active_power", "reactive_power"]
    uuids = []
    for variable_name in variable_names:
        length = 8784
        df = range(length)
        start = datetime(year=2020, month=1, day=1)
        resolution = timedelta(hours=1)
        ts = SingleTimeSeries.from_array(df, variable_name, start, resolution)
        uuids.append(ts.uuid)
        for gen in (gen1, gen2):
            system.time_series.add(ts, gen, scenario="high", model_year="2030")
            system.time_series.add(ts, gen, scenario="high", model_year="2035")
            system.time_series.add(ts, gen, scenario="low", model_year="2030")
            system.time_series.add(ts, gen, scenario="low", model_year="2035")

    system.time_series.remove(gen1, variable_name="active_power")
    system.time_series.remove(gen1, variable_name="reactive_power")
    assert not system.time_series.list_time_series(gen1, variable_name="active_power")
    assert not system.time_series.list_time_series(gen1, variable_name="reactive_power")
    assert system.time_series.list_time_series(gen2, variable_name="active_power")
    assert system.time_series.list_time_series(gen2, variable_name="reactive_power")
    system.time_series.remove(gen2)
    assert not system.time_series.list_time_series(gen2, variable_name="active_power")
    assert not system.time_series.list_time_series(gen2, variable_name="reactive_power")


def test_time_series_read_only():
    system = SimpleSystem(time_series_read_only=True)
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen = SimpleGenerator(name="gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    system.components.add(bus, gen)

    variable_name = "active_power"
    length = 8784
    df = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(df, variable_name, start, resolution)
    with pytest.raises(ISOperationNotAllowed):
        system.time_series.add(ts, gen)


def test_serialize_time_series_from_array(tmp_path):
    system = SimpleSystem()
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
    system.time_series.add(ts, gen1, gen2, scenario="high", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)

    system2 = SimpleSystem.from_json(filename, time_series_read_only=True)
    gen1b = system2.components.get(SimpleGenerator, gen1.name)
    with pytest.raises(ISOperationNotAllowed):
        system2.time_series.remove(gen1b, variable_name=variable_name)


def test_serialize_time_series(tmp_path):
    system = SimpleSystem()
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
    system.time_series.add(ts, gen1, gen2, scenario="high", model_year="2030")
    filename = tmp_path / "system.json"
    system.to_json(filename)

    system2 = SimpleSystem.from_json(filename, time_series_read_only=True)
    gen1b = system.components.get(SimpleGenerator, gen1.name)
    with pytest.raises(ISOperationNotAllowed):
        system2.time_series.remove(gen1b, variable_name=variable_name)
