# tests to run:
# Add supplemental attributes, get supplemental attributes for a component (tests associations)
# Test time series manager with supplemental attributes
# Test serialization and deserialization of supplemental attributes database
import sqlite3
from infrasys.component import Component
from infrasys.supplemental_attribute import SupplementalAttribute
from infrasys.supplemental_attribute_manager import SupplementalAttributeManager

# import infrasys.supplemental_attribute_associations
from .models.simple_system import (
    # SimpleSystem,
    SimpleBus,
    SimpleGenerator,
)


class test_supplemental_attribute(SupplementalAttribute):
    test_field: float


class test_component(Component):
    test_field: int


def test_supplemental_attribute_manager(tmp_path):
    # initialize attributes
    test_attr = test_supplemental_attribute(name="test_attribute", test_field=1.0)
    assert test_attr.test_field == 1.0

    # initialize components
    bus = SimpleBus(name="test-bus", voltage=1.1)
    gen = SimpleGenerator(
        name="gen1", active_power=1.0, rating=1.0, bus=bus, available=True
    )  # test_component(name="test", test_field=1)

    # initialize manager
    con = sqlite3.connect(tmp_path / "supp_attr.db")
    mgr = SupplementalAttributeManager(con=con)

    # add attribute to component
    mgr.add(gen, test_attr)

    # get attribute for component
    test_attr2 = mgr.get(gen, test_attr)
    assert test_attr2.test_field == test_attr.test_field

    return
