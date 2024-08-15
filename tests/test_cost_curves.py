from infrasys.cost_curves import CostCurve, FuelCurve
from infrasys.function_data import LinearFunctionData
from infrasys.value_curves import InputOutputCurve
from infrasys import Component
from .models.simple_system import SimpleSystem


class CurveComponent(Component):
    cost_curve: CostCurve


def test_cost_curve():
    # Cost curve
    cost_curve = CostCurve(
        value_curve=InputOutputCurve(
            function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
        ),
        vom_units=InputOutputCurve(
            function_data=LinearFunctionData(proportional_term=2.0, constant_term=1.0)
        ),
    )

    assert cost_curve.value_curve.function_data.proportional_term == 1.0
    assert cost_curve.vom_units.function_data.proportional_term == 2.0


def test_fuel_curve():
    # Fuel curve
    fuel_curve = FuelCurve(
        value_curve=InputOutputCurve(
            function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
        ),
        vom_units=InputOutputCurve(
            function_data=LinearFunctionData(proportional_term=2.0, constant_term=1.0)
        ),
        fuel_cost=2.5,
    )

    assert fuel_curve.value_curve.function_data.proportional_term == 1.0
    assert fuel_curve.vom_units.function_data.proportional_term == 2.0
    assert fuel_curve.fuel_cost == 2.5


def test_value_curve_custom_serialization():
    component = CurveComponent(
        name="test",
        cost_curve=CostCurve(
            value_curve=InputOutputCurve(
                function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
            ),
            vom_units=InputOutputCurve(
                function_data=LinearFunctionData(proportional_term=2.0, constant_term=1.0)
            ),
        ),
    )

    model_dump = component.model_dump(mode="json")
    assert model_dump["cost_curve"]["value_curve"]["function_data"]["proportional_term"] == 1.0

    model_dump = component.model_dump(context={"magnitude_only": True})
    assert model_dump["cost_curve"]["value_curve"]["function_data"]["proportional_term"] == 1.0

    model_dump = component.model_dump(mode="json", context={"magnitude_only": True})
    assert model_dump["cost_curve"]["value_curve"]["function_data"]["proportional_term"] == 1.0


def test_value_curve_serialization(tmp_path):
    system = SimpleSystem(auto_add_composed_components=True)

    v1 = CurveComponent(
        name="test",
        cost_curve=CostCurve(
            value_curve=InputOutputCurve(
                function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
            ),
            vom_units=InputOutputCurve(
                function_data=LinearFunctionData(proportional_term=2.0, constant_term=1.0)
            ),
        ),
    )
    system.add_component(v1)
    filename = tmp_path / "value_curve.json"

    system.to_json(filename, overwrite=True)
    system2 = SimpleSystem.from_json(filename)

    assert system2 is not None

    v2 = system2.get_component(CurveComponent, "test")

    assert v2 is not None
    assert isinstance(v1.cost_curve.value_curve.function_data, LinearFunctionData)
    assert (
        v1.cost_curve.value_curve.function_data.proportional_term
        == v2.cost_curve.value_curve.function_data.proportional_term
    )
    assert (
        v1.cost_curve.value_curve.function_data.constant_term
        == v2.cost_curve.value_curve.function_data.constant_term
    )
