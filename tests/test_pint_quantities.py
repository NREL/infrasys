import pytest
from typing import Annotated

from pydantic import ValidationError, Field
from infrasys.base_quantity import ureg
from infrasys.component import Component
from infrasys.pint_quantities import PydanticPintQuantity
from infrasys.quantities import Voltage
from pint import Quantity


class PintQuantityStrict(Component):
    voltage: Annotated[Quantity, PydanticPintQuantity("volts")]


class PintQuantityNoStrict(Component):
    voltage: Annotated[Quantity, PydanticPintQuantity("volts", strict=False)]


class PintQuantityStrictDict(Component):
    voltage: Annotated[Quantity, PydanticPintQuantity("volts", ser_mode="dict")]


class PintQuantityStrictDictPositive(Component):
    voltage: Annotated[Quantity, PydanticPintQuantity("volts", ser_mode="dict"), Field(gt=0)]


@pytest.mark.parametrize(
    "input_value",
    [10.0 * ureg.volts, Quantity(10.0, "volt"), Voltage(10.0, "volts")],
    ids=["float", "Quantity", "BaseQuantity"],
)
def test_pydantic_pint_multiple_input(input_value):
    component = PintQuantityStrict(name="TestComponent", voltage=input_value)
    assert isinstance(component.voltage, Quantity)
    assert component.voltage.magnitude == 10.0
    assert component.voltage.units == "volt"


def test_pydantic_pint_validation():
    with pytest.raises(ValidationError):
        _ = PintQuantityStrict(name="test", voltage=10.0 * ureg.meter)

    # Pass wrong type
    with pytest.raises(ValidationError):
        _ = PintQuantityStrict(name="test", voltage={10: 2})  # type: ignore


def test_compatibility_with_base_quantity():
    voltage = Voltage(10.0, "volts")
    component = PintQuantityStrict(name="TestComponent", voltage=voltage)
    assert isinstance(component.voltage, Quantity)
    assert isinstance(component.voltage, Voltage)
    assert component.voltage.magnitude == 10.0
    assert component.voltage.units == "volt"


def test_pydantic_pint_arguments():
    # Single float should work
    component = PintQuantityNoStrict(name="TestComponent", voltage=10.0)  # type: ignore
    assert isinstance(component.voltage, Quantity)
    assert component.voltage.magnitude == 10.0
    assert component.voltage.units == "volt"

    with pytest.raises(ValidationError):
        _ = PintQuantityStrictDictPositive(name="TestComponent", voltage=-10)  # type: ignore


def test_serialization():
    component = PintQuantityStrict(name="TestComponent", voltage=10.0 * ureg.volts)
    component_serialized = component.model_dump()
    assert isinstance(component_serialized["voltage"], Quantity)
    assert component_serialized["voltage"].magnitude == 10.0
    assert component_serialized["voltage"].units == "volt"

    component_json = component.model_dump(mode="json")
    assert component_json["voltage"] == "10.0 volt"

    component_dict = component.model_dump(mode="dict")
    assert isinstance(component_dict["voltage"], dict)
    assert component_dict["voltage"].get("magnitude", False)
    assert component_dict["voltage"].get("units", False)
    assert component_dict["voltage"]["magnitude"] == 10.0
    assert str(component_dict["voltage"]["units"]) == "volt"

    component = PintQuantityStrict(name="TestComponent", voltage=10.0 * ureg.volts)
    component_json = component.model_dump(mode="json")
    assert isinstance(component_dict["voltage"], dict)
    assert component_dict["voltage"].get("magnitude", False)
    assert component_dict["voltage"].get("units", False)
    assert component_dict["voltage"]["magnitude"] == 10.0
    assert component_dict["voltage"]["units"] == "volt"
