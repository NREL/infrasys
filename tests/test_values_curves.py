from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
    PiecewiseStepData,
)
from infrasys.value_curves import (
    InputOutputCurve,
    IncrementalCurve,
    AverageRateCurve,
)
from infrasys import Component
from infrasys.exceptions import ISOperationNotAllowed
from .models.simple_system import SimpleSystem
import pytest


class ValueCurveComponent(Component):
    value_curve: InputOutputCurve


def test_input_output_curve():
    curve = InputOutputCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=1.0)
    )

    assert isinstance(curve, InputOutputCurve)
    assert isinstance(curve.function_data, LinearFunctionData)


def test_incremental_curve():
    curve = IncrementalCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=1.0),
        initial_input=1.0,
    )

    assert isinstance(curve, IncrementalCurve)
    assert isinstance(curve.function_data, LinearFunctionData)


def test_average_rate_curve():
    curve = AverageRateCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=1.0),
        initial_input=1.0,
    )

    assert isinstance(curve, AverageRateCurve)
    assert isinstance(curve.function_data, LinearFunctionData)


def test_average_rate_conversion():
    # Linear function data
    curve = AverageRateCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0),
        initial_input=None,
    )
    with pytest.raises(ISOperationNotAllowed):
        curve.to_input_output()

    curve.initial_input = 0.0
    new_curve = curve.to_input_output()
    assert isinstance(new_curve, InputOutputCurve)
    assert isinstance(new_curve.function_data, QuadraticFunctionData)
    assert new_curve.function_data.quadratic_term == 1.0

    curve.function_data.proportional_term = 0.0
    new_curve = curve.to_input_output()
    assert isinstance(new_curve, InputOutputCurve)
    assert isinstance(new_curve.function_data, LinearFunctionData)
    assert new_curve.function_data.proportional_term == 2.0

    # Piecewise step data
    data = PiecewiseStepData(x_coords=[1.0, 3.0, 5.0], y_coords=[2.0, 6.0])
    curve = AverageRateCurve(function_data=data, initial_input=None)
    with pytest.raises(ISOperationNotAllowed):
        curve.to_input_output()

    curve.initial_input = 0.0
    new_curve = curve.to_input_output()
    assert isinstance(new_curve, InputOutputCurve)


def test_incremental_conversion():
    # Linear function data
    curve = IncrementalCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=1.0),
        initial_input=None,
    )
    with pytest.raises(ISOperationNotAllowed):
        curve.to_input_output()
    curve.initial_input = 0.0
    new_curve = curve.to_input_output()
    assert isinstance(new_curve, InputOutputCurve)
    assert isinstance(new_curve.function_data, QuadraticFunctionData)
    assert new_curve.function_data.quadratic_term == 0.5

    curve.function_data.proportional_term = 0.0
    new_curve = curve.to_input_output()
    assert isinstance(new_curve, InputOutputCurve)
    assert isinstance(new_curve.function_data, LinearFunctionData)
    assert new_curve.function_data.proportional_term == 1.0

    # Piecewise step data
    data = PiecewiseStepData(x_coords=[1.0, 3.0, 5.0], y_coords=[2.0, 6.0])
    curve = IncrementalCurve(function_data=data, initial_input=None)
    with pytest.raises(ISOperationNotAllowed):
        curve.to_input_output()
    curve.initial_input = 0.0
    new_curve = curve.to_input_output()
    assert isinstance(new_curve, InputOutputCurve)


def test_value_curve_custom_serialization():
    component = ValueCurveComponent(
        name="test",
        value_curve=InputOutputCurve(
            function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
        ),
    )

    model_dump = component.model_dump(mode="json")
    assert model_dump["value_curve"]["function_data"]["proportional_term"] == 1.0

    model_dump = component.model_dump(context={"magnitude_only": True})
    assert model_dump["value_curve"]["function_data"]["proportional_term"] == 1.0

    model_dump = component.model_dump(mode="json", context={"magnitude_only": True})
    assert model_dump["value_curve"]["function_data"]["proportional_term"] == 1.0


def test_value_curve_serialization(tmp_path):
    system = SimpleSystem(auto_add_composed_components=True)

    v1 = ValueCurveComponent(
        name="test",
        value_curve=InputOutputCurve(
            function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
        ),
    )
    system.add_component(v1)
    filename = tmp_path / "value_curve.json"

    system.to_json(filename, overwrite=True)
    system2 = SimpleSystem.from_json(filename)

    assert system2 is not None

    v2 = system2.get_component(ValueCurveComponent, "test")

    assert v2 is not None
    assert isinstance(v1.value_curve.function_data, LinearFunctionData)
    assert (
        v1.value_curve.function_data.proportional_term
        == v2.value_curve.function_data.proportional_term
    )
    assert v1.value_curve.function_data.constant_term == v2.value_curve.function_data.constant_term
