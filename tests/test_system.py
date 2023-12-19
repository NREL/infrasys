import pytest

from infra_sys.exceptions import ISNotStored
from infra_sys.geography_coordinates import GeographyCoordinates
from infra_sys.time_series_models import SingleTimeSeries
from simple_system import SimpleSystem, SimpleBus, SimpleGenerator, SimpleSubsystem


def test_system(tmp_path):
    sys = SimpleSystem()
    geo = GeographyCoordinates(latitude=1.0, longitude=2.0)
    bus = SimpleBus(name="test-bus", voltage=1.1, coordinates=geo)
    gen = SimpleGenerator(name="test-gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    subsystem = SimpleSubsystem(name="test-subsystem", generators=[gen])
    sys.components.add(geo, bus, gen, subsystem)

    gen2 = sys.components.get(SimpleGenerator, "test-gen")
    assert gen2 is gen
    assert gen2.bus is bus

    filename = tmp_path / "sys.json"
    sys.to_json(filename, overwrite=True, indent=2)
    sys2 = SimpleSystem.from_json(filename)
    assert sys2.components.get_by_uuid(geo.uuid) == geo
    assert sys2.components.get(SimpleBus, "test-bus") == bus
    assert sys2.components.get(SimpleGenerator, "test-gen") == gen
    assert sys2.components.get(SimpleSubsystem, "test-subsystem") == subsystem


def test_in_memory_time_series(hourly_time_array):
    sys = SimpleSystem()
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen = SimpleGenerator(name="test-gen", active_power=1.0, rating=1.0, bus=bus, available=True)
    sys.components.add(bus, gen)

    name = "active_power"
    df = hourly_time_array
    ts = SingleTimeSeries.from_time_array(df, name)
    sys.time_series.add(ts, [gen])
    ts2 = sys.time_series.get(gen, name)
    assert ts2 == ts

    with pytest.raises(ISNotStored):
        sys.time_series.get(gen, "invalid")
