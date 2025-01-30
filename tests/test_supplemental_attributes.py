from infrasys import GeographicInfo, SupplementalAttribute
from .models.simple_system import (
    SimpleSystem,
    SimpleBus,
    SimpleGenerator,
)


def test_supplemental_attribute_manager(tmp_path):
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    attr = GeographicInfo.example()
    system = SimpleSystem(auto_add_composed_components=True)
    system.add_component(gen)
    system.add_supplemental_attribute(bus, attr)

    for attr_type in (GeographicInfo, SupplementalAttribute):
        attrs = list(system.get_supplemental_attributes(attr_type))
        assert len(attrs) == 1
        assert attrs[0] is attr

    assert system.get_supplemental_attribute_by_uuid(attr.uuid) is attr

    components = system.get_components_with_supplemental_attribute(attr)
    assert len(components) == 1
    assert components[0] is bus

    attributes = system.get_supplemental_attributes_with_component(bus)
    assert len(attributes) == 1
    assert attributes[0] is attr

    path = tmp_path / "system"
    system.save(path)
    system_file = path / "system.json"
    assert system_file.exists()

    system.remove_supplemental_attribute(attr)
    assert not system.get_supplemental_attributes_with_component(bus)
    for attr_type in (GeographicInfo, SupplementalAttribute):
        assert not list(system.get_supplemental_attributes(attr_type))

    SimpleSystem.from_json(system_file)
    # TODO: not working
    # attrs = list(system2.get_supplemental_attributes(GeographicInfo))
    # assert len(attrs) == 1
    # assert attrs[0] == attr
