# tests to run:
# Add supplemental attributes, get supplemental attributes for a component (tests associations)
# Test time series manager with supplemental attributes
# Test serialization and deserialization of supplemental attributes database
from infrasys.supplemental_attribute import SupplementalAttribute
# import infrasys.supplemental_attribute_associations


class test_supplemental_attribute(SupplementalAttribute):
    test_field: float


def test_supplemental_attribute_manager():
    test_attribute = test_supplemental_attribute(name="test", test_field=1.0)

    assert test_attribute.test_field == 1.0

    return
