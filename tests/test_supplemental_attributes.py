# tests to run:
# Add supplemental attributes, get supplemental attributes for a component (tests associations)
# Test time series manager with supplemental attributes
# Test serialization and deserialization of supplemental attributes database
import sqlite3
from infrasys.component import Component
from infrasys.supplemental_attribute import SupplementalAttribute
from infrasys.supplemental_attribute_manager import SupplementalAttributeManager
# import infrasys.supplemental_attribute_associations


class test_supplemental_attribute(SupplementalAttribute):
    test_field: float


class test_component(Component):
    t: int


def test_supplemental_attribute_manager(tmp_path):
    # system = SimpleSystem(auto_add_composed_components=True)

    test_attribute = test_supplemental_attribute(name="test_attribute", test_field=1.0)
    # test_component = Component(name="test_component")
    con = sqlite3.connect(tmp_path / "supp_attr.db")

    pls_work = test_component(name="test", t=1)

    # initialize manager
    mgr = SupplementalAttributeManager(con=con)

    # add attribute to component
    mgr.add(pls_work, test_attribute)

    assert test_attribute.test_field == 1.0

    return
