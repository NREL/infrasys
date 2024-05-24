from pydantic import ValidationError
from infrasys.base_quantity import ureg, BaseQuantity
from infrasys.component import Component
from infrasys.quantities import ActivePower, Time, Voltage
from pint.errors import DimensionalityError
import pytest
import numpy as np


class BaseQuantityComoponent(Component):
    voltage: Voltage


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


def test_base_unit_validation():
    test_magnitude = 100
    test_unit = "volt"

    class Voltage(BaseQuantity):
        __base_unit__ = "volt"

    test_quantity = Voltage(test_magnitude, test_unit)

    test_component = BaseQuantityComoponent(name="testing", voltage=test_quantity)

    assert test_component.voltage == test_quantity
    assert test_component.voltage.magnitude == test_magnitude
    assert test_component.voltage.units == test_unit

    with pytest.raises(ValidationError):
        BaseQuantityComoponent(name="test", voltage=Voltage(test_magnitude, "meter"))


@pytest.mark.parametrize("input_unit", [Voltage(10, "kV"), 10 * ureg.volt, 10, 10.0])
def test_different_validate(input_unit):
    test_component = BaseQuantityComoponent(name="test", voltage=input_unit)
    assert isinstance(test_component.voltage, BaseQuantity)
    assert test_component.voltage.magnitude == 10
    assert test_component.voltage.check(Voltage.__base_unit__)


def test_custom_serialization():
    component = BaseQuantityComoponent(name="test", voltage=10.0)

    model_dump = component.model_dump(mode="json")

    assert model_dump["voltage"] == str(Voltage(10.0, "volt"))

    model_dump = component.model_dump(mode="json", context={"magnitude_only": True})
    assert model_dump["voltage"] == str(10.0)
