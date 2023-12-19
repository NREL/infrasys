from pathlib import Path

from infra_sys.geography_coordinates import GeographyCoordinates
from simple_system import SimpleSystem, SimpleBus, SimpleGenerator, SimpleSubsystem


def test_system():
    sys = SimpleSystem()
    geo = GeographyCoordinates(latitude=1.0, longitude=2.0)
    bus = SimpleBus(name="test-bus", voltage=1.1, coordinates=geo)
    gen = SimpleGenerator(name="test-gen", rating=1.0, bus=bus, available=True)
    subsystem = SimpleSubsystem(name="test-subsystem", generators=[gen])
    sys.components.add(geo, bus, gen, subsystem)

    gen2 = sys.components.get(SimpleGenerator, "test-gen")
    assert gen2 is gen
    assert gen2.bus is bus
    filename = Path("sys.json")
    sys.to_json(filename, overwrite=True, indent=2)
    sys2 = SimpleSystem.from_json(filename)
    gen3 = sys2.components.get(SimpleGenerator, "test-gen")
    assert gen3 == gen2
