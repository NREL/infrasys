from uuid import uuid4

import pytest

from infra_sys.exceptions import ISNotStored, ISOperationNotAllowed
from infra_sys.geo_location import GeoLocation
from infra_sys.component_models import Component
from infra_sys.time_series_models import SingleTimeSeries
from simple_system import (
    GeneratorBase,
    SimpleSystem,
    SimpleBus,
    SimpleGenerator,
    SimpleSubsystem,
    RenewableGenerator,
)


def test_system():
    sys = SimpleSystem()
    geo = GeoLocation(x=1.0, y=2.0)
    bus = SimpleBus(name="test-bus", voltage=1.1, coordinates=geo)
    gen = SimpleGenerator(name="test-gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    subsystem = SimpleSubsystem(name="test-subsystem", generators=[gen])
    sys.components.add(geo, bus, gen, subsystem)

    gen2 = sys.components.get(SimpleGenerator, "test-gen")
    assert gen2 is gen
    assert gen2.bus is bus


def test_serialization(tmp_path, simple_system):
    sys = simple_system
    geos = list(sys.components.iter(GeoLocation))
    assert len(geos) == 1
    geo = geos[0]
    bus = sys.components.get(SimpleBus, "test-bus")
    gen = sys.components.get(SimpleGenerator, "test-gen")
    subsystem = sys.components.get(SimpleSubsystem, "test-subsystem")

    filename = tmp_path / "sys.json"
    sys.to_json(filename, overwrite=True, indent=2)
    sys2 = SimpleSystem.from_json(filename)
    assert sys2.components.get_by_uuid(geo.uuid) == geo
    assert sys2.components.get(SimpleBus, "test-bus") == bus
    assert sys2.components.get(SimpleGenerator, "test-gen") == gen
    assert sys2.components.get(SimpleSubsystem, "test-subsystem") == subsystem


def test_get_components(simple_system):
    sys = simple_system
    for _ in range(5):
        gen = RenewableGenerator.example()
        sys.add_component(gen)
    all_components = list(sys.get_components(Component))
    assert len(all_components) == 9
    generators = list(
        sys.get_components(GeneratorBase, filter_func=lambda x: x.name == "renewable-gen")
    )
    assert len(generators) == 5

    with pytest.raises(ISOperationNotAllowed):
        sys.get_component(RenewableGenerator, "renewable-gen")

    assert len(list(sys.list_components_by_name(RenewableGenerator, "renewable-gen"))) == 5

    with pytest.raises(ISNotStored):
        sys.components.get_by_uuid(uuid4())


def test_in_memory_time_series(hourly_time_array):
    sys = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen1 = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    gen2 = SimpleGenerator(name="gen2", active_power=1.0, rating=1.0, bus=bus, available=True)
    sys.add_components(bus, gen1, gen2)

    name = "active_power"
    df = hourly_time_array
    ts = SingleTimeSeries.from_dataframe(df, name)
    sys.time_series.add(ts, [gen1, gen2])
    assert gen1.has_time_series(name)
    assert gen2.has_time_series(name)
    assert sys.time_series.get(gen1, name) == ts
    assert sys.time_series.get(gen2, name) == ts

    sys.time_series.remove([gen1], name)
    with pytest.raises(ISNotStored):
        sys.time_series.get(gen1, name)

    assert sys.time_series.get(gen2, name) == ts
    sys.time_series.remove([gen2], name)
    with pytest.raises(ISNotStored):
        sys.time_series.get(gen2, name)

    assert not gen1.has_time_series(name)
    assert not gen2.has_time_series(name)
