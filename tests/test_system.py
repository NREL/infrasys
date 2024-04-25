import itertools
import os
from datetime import timedelta, datetime
from uuid import uuid4

import numpy as np
import pytest

from infrasys.exceptions import (
    ISAlreadyAttached,
    ISNotStored,
    ISOperationNotAllowed,
    ISConflictingArguments,
)
from infrasys import Component, Location, SingleTimeSeries
from infrasys.quantities import ActivePower
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

    gen = all_components[0]
    assert system.get_component_by_uuid(gen.uuid) is gen
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


def test_get_component_by_label():
    system = SimpleSystem(auto_add_composed_components=True)
    gen = RenewableGenerator.example()
    system.add_component(gen)
    assert system.get_component_by_label(gen.label) is gen
    with pytest.raises(ISNotStored):
        system.get_component_by_label("SimpleGenerator.invalid")
    with pytest.raises(ISNotStored):
        system.get_component_by_label("invalid.invalid")
    coordinates = gen.bus.coordinates
    assert coordinates is not None
    assert system.get_component_by_label(coordinates.label) is coordinates

    gen = RenewableGenerator.example()
    system.add_component(gen)
    with pytest.raises(ISOperationNotAllowed):
        system.get_component_by_label(gen.label)


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
    assert system.has_time_series(gen1, variable_name=variable_name)
    assert system.has_time_series(gen2, variable_name=variable_name)
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
    with pytest.raises(ValueError, match="The first argument must"):
        # Test a common mistake.
        system.add_time_series(gen1, ts)

    system.add_time_series(ts, gen1, gen2)
    assert system.has_time_series(gen1, variable_name=variable_name)
    assert system.has_time_series(gen2, variable_name=variable_name)
    assert system.get_time_series(gen1, variable_name=variable_name) == ts
    system.remove_time_series(gen1, gen2, variable_name=variable_name)
    with pytest.raises(ISNotStored):
        system.get_time_series(gen1, variable_name=variable_name)

    assert not system.has_time_series(gen1, variable_name=variable_name)
    assert not system.has_time_series(gen2, variable_name=variable_name)


@pytest.mark.parametrize(
    "params", list(itertools.product([True, False], [True, False], [True, False]))
)
def test_time_series_retrieval(params):
    in_memory, use_quantity, sql_json = params
    try:
        if not sql_json:
            os.environ["__INFRASYS_NON_JSON_SQLITE__"] = "1"
        system = SimpleSystem(time_series_in_memory=in_memory)
        bus = SimpleBus(name="test-bus", voltage=1.1)
        gen = SimpleGenerator(name="gen", active_power=1.0, rating=1.0, bus=bus, available=True)
        system.add_components(bus, gen)

        length = 10
        initial_time = datetime(year=2020, month=1, day=1)
        time_array = [initial_time + timedelta(hours=i) for i in range(length)]
        data = (
            [ActivePower(np.random.rand(length), "watts") for _ in range(4)]
            if use_quantity
            else [np.random.rand(length) for _ in range(4)]
        )
        variable_name = "active_power"
        ts1 = SingleTimeSeries.from_time_array(data[0], variable_name, time_array)
        ts2 = SingleTimeSeries.from_time_array(data[1], variable_name, time_array)
        ts3 = SingleTimeSeries.from_time_array(data[2], variable_name, time_array)
        ts4 = SingleTimeSeries.from_time_array(data[3], variable_name, time_array)
        system.add_time_series(ts1, gen, scenario="high", model_year="2030")
        system.add_time_series(ts2, gen, scenario="high", model_year="2035")
        system.add_time_series(ts3, gen, scenario="low", model_year="2030")
        system.add_time_series(ts4, gen, scenario="low", model_year="2035")
        assert len(system.list_time_series_metadata(gen)) == 4
        assert len(system.list_time_series_metadata(gen, scenario="high", model_year="2035")) == 1
        assert (
            system.list_time_series_metadata(gen, scenario="high", model_year="2035")[
                0
            ].user_attributes["model_year"]
            == "2035"
        )
        assert len(system.list_time_series_metadata(gen, scenario="low")) == 2
        for metadata in system.list_time_series_metadata(gen, scenario="high"):
            assert metadata.user_attributes["scenario"] == "high"

        assert (
            system.get_time_series(
                gen, variable_name=variable_name, scenario="high", model_year="2030"
            )
            == ts1
        )
        assert (
            system.get_time_series(
                gen, variable_name=variable_name, scenario="high", model_year="2035"
            )
            == ts2
        )
        assert (
            system.get_time_series(
                gen, variable_name=variable_name, scenario="low", model_year="2030"
            )
            == ts3
        )
        assert (
            system.get_time_series(
                gen, variable_name=variable_name, scenario="low", model_year="2035"
            )
            == ts4
        )

        with pytest.raises(ISAlreadyAttached):
            system.add_time_series(ts4, gen, scenario="low", model_year="2035")

        assert system.has_time_series(gen, variable_name=variable_name)
        assert system.has_time_series(gen, variable_name=variable_name, scenario="high")
        assert system.has_time_series(
            gen, variable_name=variable_name, scenario="high", model_year="2030"
        )
        assert not system.has_time_series(gen, variable_name=variable_name, model_year="2036")
        assert (
            system.get_time_series(
                gen, variable_name=variable_name, scenario="high", model_year="2030"
            )
            == ts1
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
        assert not system.has_time_series(gen, variable_name=variable_name)
    finally:
        os.environ.pop("__INFRASYS_NON_JSON_SQLITE__", None)


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
    gen1b = system2.get_component(SimpleGenerator, gen1.name)
    with pytest.raises(ISOperationNotAllowed):
        system2.remove_time_series(gen1b, variable_name=variable_name)
    ts2 = system.get_time_series(gen1b, variable_name=variable_name)
    assert ts2.data.tolist() == list(data)


@pytest.mark.parametrize("in_memory", [True, False])
def test_time_series_slices(in_memory):
    system = SimpleSystem(
        name="test-system",
        auto_add_composed_components=True,
        time_series_in_memory=in_memory,
    )
    gen = SimpleGenerator.example()
    system.add_components(gen)
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
    assert gen2.bus is gen1.bus

    gen3 = system.copy_component(gen1, name="gen3")
    assert gen3.name == "gen3"

    gen4 = system.copy_component(gen1, name="gen4", attach=True)
    assert gen4.name == "gen4"


def test_deepcopy_component(simple_system_with_time_series: SimpleSystem):
    system = simple_system_with_time_series
    gen1 = system.get_component(SimpleGenerator, "test-gen")
    subsystem = SimpleSubsystem(name="subsystem1", generators=[gen1])
    system.add_component(subsystem)
    gen2 = system.deepcopy_component(gen1)
    assert gen2.name == gen1.name
    assert gen2.uuid == gen1.uuid
    assert gen2.bus.uuid == gen1.bus.uuid
    assert gen2.bus is not gen1.bus


@pytest.mark.parametrize("in_memory", [True, False])
def test_remove_component(in_memory):
    system = SimpleSystem(
        name="test-system",
        auto_add_composed_components=True,
        time_series_in_memory=in_memory,
    )
    gen1 = SimpleGenerator.example()
    system.add_components(gen1)
    gen2 = system.copy_component(gen1, name="gen2", attach=True)
    variable_name = "active_power"
    length = 8784
    data = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    system.add_time_series(ts, gen1, gen2)

    system.remove_component_by_name(type(gen1), gen1.name)
    assert not system.has_time_series(gen1)
    assert system.has_time_series(gen2)

    system.remove_component_by_uuid(gen2.uuid)
    assert not system.has_time_series(gen2)

    with pytest.raises(ISNotStored):
        system.remove_component(gen2)

    with pytest.raises(ISNotStored):
        system.remove_component(gen2)

    for gen in (gen1, gen2):
        with pytest.raises(ISNotStored):
            system.get_component(SimpleGenerator, gen.name)


def test_system_to_dict():
    system = SimpleSystem(
        name="test-system",
        auto_add_composed_components=True,
    )
    gen1 = SimpleGenerator.example()
    gen2 = SimpleGenerator.example()
    gen3 = SimpleGenerator.example()
    system.add_components(gen1, gen2, gen3)

    component_dict: list[dict] = list(system.to_records(SimpleGenerator))
    assert len(component_dict) == 3  # 3 generators
    assert component_dict[0].get("uuid") is not None
    assert component_dict[0]["bus"] == gen1.bus.label

    exclude_first_level_fields = {"name": True, "available": True}
    component_dict = list(system.to_records(SimpleGenerator, exclude=exclude_first_level_fields))
    assert len(component_dict) == 3
    assert component_dict[0].get("name", None) is None
    assert component_dict[0].get("available", None) is None

    component_dict = list(system.to_records(SimpleGenerator))
    assert len(component_dict) == 3  # 3 generators
    assert component_dict[0]["bus"] == gen1.bus.label

    variable_name = "active_power"
    length = 8784
    data = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    system.add_time_series(ts, gen1)
    component_dicts = list(system.to_records(SimpleGenerator))
    assert len(component_dicts) == 3  # 3 generators


def test_time_series_metadata_sql():
    system = SimpleSystem(name="test-system", auto_add_composed_components=True)
    gen1 = SimpleGenerator.example()
    system.add_components(gen1)
    gen2 = system.copy_component(gen1, name="gen2", attach=True)
    variable_name = "active_power"
    length = 8784
    data = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts1 = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    ts2 = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    system.add_time_series(ts1, gen1)
    system.add_time_series(ts2, gen2)
    rows = system.time_series.metadata_store.sql(
        f"""
        SELECT component_type, time_series_type, component_uuid, time_series_uuid
        FROM {system.time_series.metadata_store.TABLE_NAME}
        WHERE component_uuid = '{gen1.uuid}'
    """
    )
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == SimpleGenerator.__name__
    assert row[1] == SingleTimeSeries.__name__
    assert row[2] == str(gen1.uuid)
    assert row[3] == str(ts1.uuid)


def test_time_series_metadata_list_rows():
    system = SimpleSystem(name="test-system", auto_add_composed_components=True)
    gen1 = SimpleGenerator.example()
    system.add_components(gen1)
    gen2 = system.copy_component(gen1, name="gen2", attach=True)
    variable_name = "active_power"
    length = 8784
    data = range(length)
    start = datetime(year=2020, month=1, day=1)
    resolution = timedelta(hours=1)
    ts1 = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    ts2 = SingleTimeSeries.from_array(data, variable_name, start, resolution)
    system.add_time_series(ts1, gen1)
    system.add_time_series(ts2, gen2)
    columns = [
        "component_type",
        "time_series_type",
        "component_uuid",
        "time_series_uuid",
    ]
    rows = system.time_series.metadata_store.list_rows(
        gen2,
        variable_name=variable_name,
        time_series_type=SingleTimeSeries.__name__,
        columns=columns,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row[0] == SimpleGenerator.__name__
    assert row[1] == SingleTimeSeries.__name__
    assert row[2] == str(gen2.uuid)
    assert row[3] == str(ts2.uuid)


def test_system_counts():
    system = SimpleSystem(name="test-system", auto_add_composed_components=True)
    gen1 = SimpleGenerator.example()
    gen2 = SimpleGenerator.example()
    system.add_components(gen1, gen2)
    variable_name = "active_power"
    data = range(10)

    def add_time_series(iteration, initial_time, resolution):
        for i in range(5):
            ts1 = SingleTimeSeries.from_array(
                data,
                f"{variable_name}_{iteration}_{i}",
                initial_time + resolution * i,
                resolution,
            )
            ts2 = SingleTimeSeries.from_array(
                data,
                f"{variable_name}_{iteration}_{i}",
                initial_time + resolution * i,
                resolution,
            )
            system.add_time_series(ts1, gen1, gen2)
            system.add_time_series(ts2, gen1.bus)

    add_time_series(1, datetime(year=2020, month=1, day=1), timedelta(hours=1))
    add_time_series(2, datetime(year=2020, month=2, day=1), timedelta(minutes=5))

    # 2 generators, 2 buses, 2 locations
    assert system._components.get_num_components() == 6
    components_by_type = system._components.get_num_components_by_type()
    assert components_by_type[SimpleGenerator] == 2
    assert components_by_type[SimpleBus] == 2
    ts_counts = system.time_series.metadata_store.get_time_series_counts()
    assert ts_counts.time_series_count == 2 * 10
    assert (
        ts_counts.time_series_type_count[
            ("SimpleGenerator", "SingleTimeSeries", "2020-01-01 02:00:00", "1:00:00")
        ]
        == 2
    )
    assert (
        ts_counts.time_series_type_count[
            ("SimpleBus", "SingleTimeSeries", "2020-02-01 00:10:00", "0:05:00")
        ]
        == 1
    )


def test_system_printing(simple_system_with_time_series):
    simple_system_with_time_series.info()
