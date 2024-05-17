from infrasys.base_quantity import ureg, BaseQuantity
from infrasys.quantities import ActivePower, Time
from pint.errors import DimensionalityError
import pytest
import numpy as np


def test_base_quantity():
    distance_quantity = ureg.check(None, "meter")(BaseQuantity)

    unit = distance_quantity(100, "meter")
    assert isinstance(unit, BaseQuantity)

    # Check that we can not assign units that are not-related.
    with pytest.raises(DimensionalityError):
        _ = distance_quantity(100, "kWh")

    # Check unit multiplication
    active_power_quantity = ActivePower(100, "kW")
    hours = Time(2, "h")

    result_quantity = active_power_quantity * hours
    assert result_quantity.check("[energy]")
    assert result_quantity.magnitude == 200

    # Check to dict
    assert result_quantity.to_dict() == {
        "value": result_quantity.magnitude,
        "units": str(result_quantity.units),
    }


def test_base_quantity_numpy():
    array = np.arange(0, 10)
    measurements = ActivePower(array, "kW")
    assert isinstance(measurements, BaseQuantity)
    assert measurements.to_dict()["value"] == array.tolist()


def test_unit_deserialization():
    test_units = {
        "value": 100,
        "units": "kilowatt",  # The unit name should be the pint default name
    }
    active_power = BaseQuantity.from_dict(test_units)
    assert isinstance(active_power, BaseQuantity)
    assert active_power.magnitude == 100
    assert str(active_power.units) == "kilowatt"
