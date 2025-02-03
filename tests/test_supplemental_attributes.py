from infrasys import GeographicInfo, SupplementalAttribute
from .models.simple_system import (
    SimpleSystem,
    SimpleBus,
    SimpleGenerator,
)


def test_supplemental_attribute_manager(tmp_path):
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen = SimpleGenerator(name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True)
    attr1 = GeographicInfo.example()
    attr2 = GeographicInfo.example()
    attr2.geo_json["geometry"]["coordinates"] = [1.0, 2.0]
    system = SimpleSystem(auto_add_composed_components=True)
    system.add_component(gen)
    system.add_supplemental_attribute(bus, attr1)
    system.add_supplemental_attribute(bus, attr2)

    def check_attrs(attrs):
        assert len(attrs) == 2
        assert attrs[0] == attr1 or attrs[0] == attr2
        assert attrs[1] == attr1 or attrs[1] == attr2

    for attr_type in (GeographicInfo, SupplementalAttribute):
        attrs = list(system.get_supplemental_attributes(attr_type))
        check_attrs(attrs)

    assert system.get_supplemental_attribute_by_uuid(attr1.uuid) is attr1

    components = system.get_components_with_supplemental_attribute(attr1)
    assert len(components) == 1
    assert components[0] is bus

    attrs = system.get_supplemental_attributes_with_component(bus)
    check_attrs(attrs)

    attrs = list(
        system.get_supplemental_attributes(
            GeographicInfo,
            filter_func=lambda x: x.geo_json["geometry"]["coordinates"] == [1.0, 2.0],
        )
    )
    assert len(attrs) == 1
    assert attrs[0] == attr2

    attrs = system.get_supplemental_attributes_with_component(
        bus,
        attribute_type=GeographicInfo,
        filter_func=lambda x: x.geo_json["geometry"]["coordinates"] == [1.0, 2.0],
    )
    assert len(attrs) == 1
    assert attrs[0] == attr2

    path = tmp_path / "system"
    system.save(path)
    system_file = path / "system.json"
    assert system_file.exists()

    system.remove_supplemental_attribute(attr1)
    system.remove_supplemental_attribute(attr2)
    assert not system.get_supplemental_attributes_with_component(bus)
    for attr_type in (GeographicInfo, SupplementalAttribute):
        assert not list(system.get_supplemental_attributes(attr_type))

    system2 = SimpleSystem.from_json(system_file)
    attrs = list(system2.get_supplemental_attributes(GeographicInfo))
    check_attrs(attrs)
