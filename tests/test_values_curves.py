from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
    PiecewiseStepData,
    PiecewiseLinearData,
    XYCoords,
)
from infrasys.value_curves import (
    InputOutputCurve,
    IncrementalCurve,
    AverageRateCurve,
    InputOutputLinearToQuadratic,
    InputOutputToAverageRate,
    InputOutputToIncremental,
    IncrementalToInputOutput,
    IncrementalToAverageRate,
    AverageRateToInputOutput,
    AverageRateToIncremental,
)
from infrasys import Component
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


def test_input_output_conversion():
    # Linear function data
    curve = InputOutputCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=1.0)
    )
    new_curve = InputOutputToAverageRate(curve)
    assert isinstance(new_curve, AverageRateCurve)

    new_curve = InputOutputToIncremental(curve)
    assert isinstance(new_curve, IncrementalCurve)

    new_curve = InputOutputLinearToQuadratic(curve)
    assert isinstance(new_curve.function_data, QuadraticFunctionData)

    # Quadratic function data
    q = 3.0
    p = 2.0
    c = 1.0

    curve = InputOutputCurve(
        function_data=QuadraticFunctionData(quadratic_term=q, proportional_term=p, constant_term=c)
    )
    new_curve = InputOutputToAverageRate(curve)
    assert isinstance(new_curve, AverageRateCurve)
    assert new_curve.function_data.proportional_term == q

    new_curve = InputOutputToIncremental(curve)
    assert isinstance(new_curve, IncrementalCurve)
    assert new_curve.function_data.proportional_term == 2 * q

    # Piecewise linear data
    xy = [XYCoords(1.0, 2.0), XYCoords(2.0, 4.0), XYCoords(4.0, 10.0)]

    curve = InputOutputCurve(function_data=PiecewiseLinearData(points=xy))
    new_curve = InputOutputToAverageRate(curve)
    assert isinstance(new_curve, AverageRateCurve)
    assert new_curve.function_data.y_coords == [2.0, 2.5]

    new_curve = InputOutputToIncremental(curve)
    assert isinstance(new_curve, IncrementalCurve)


def test_incremental_conversion():
    # Linear function data
    curve = IncrementalCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=1.0),
        initial_input=None,
    )
    with pytest.raises(ValueError):
        IncrementalToInputOutput(curve)

    curve.initial_input = 0.0
    new_curve = IncrementalToInputOutput(curve)
    assert isinstance(new_curve, InputOutputCurve)
    assert isinstance(new_curve.function_data, QuadraticFunctionData)
    assert new_curve.function_data.quadratic_term == 0.5

    new_curve = IncrementalToAverageRate(curve)
    assert isinstance(new_curve, AverageRateCurve)

    curve.function_data.proportional_term = 0.0
    new_curve = IncrementalToInputOutput(curve)
    assert isinstance(new_curve, InputOutputCurve)
    assert isinstance(new_curve.function_data, LinearFunctionData)
    assert new_curve.function_data.proportional_term == 1.0

    # Piecewise step data
    data = PiecewiseStepData(x_coords=[1.0, 3.0, 5.0], y_coords=[2.0, 6.0])
    curve = IncrementalCurve(function_data=data, initial_input=None)
    with pytest.raises(ValueError):
        IncrementalToInputOutput(curve)

    curve.initial_input = 0.0
    new_curve = IncrementalToInputOutput(curve)
    assert isinstance(new_curve, InputOutputCurve)

    new_curve = IncrementalToAverageRate(curve)
    assert isinstance(new_curve, AverageRateCurve)


def test_average_rate_conversion():
    # Linear function data
    curve = AverageRateCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0),
        initial_input=None,
    )
    with pytest.raises(ValueError):
        AverageRateToInputOutput(curve)

    curve.initial_input = 0.0
    new_curve = AverageRateToInputOutput(curve)
    assert isinstance(new_curve, InputOutputCurve)
    assert isinstance(new_curve.function_data, QuadraticFunctionData)
    assert new_curve.function_data.quadratic_term == 1.0

    new_curve = AverageRateToIncremental(curve)
    assert isinstance(new_curve, IncrementalCurve)

    curve.function_data.proportional_term = 0.0
    new_curve = AverageRateToInputOutput(curve)
    assert isinstance(new_curve, InputOutputCurve)
    assert isinstance(new_curve.function_data, LinearFunctionData)
    assert new_curve.function_data.proportional_term == 2.0

    # Piecewise step data
    data = PiecewiseStepData(x_coords=[1.0, 3.0, 5.0], y_coords=[2.0, 6.0])
    curve = AverageRateCurve(function_data=data, initial_input=None)
    with pytest.raises(ValueError):
        AverageRateToInputOutput(curve)

    curve.initial_input = 0.0
    new_curve = AverageRateToInputOutput(curve)
    assert isinstance(new_curve, InputOutputCurve)

    new_curve = AverageRateToIncremental(curve)
    assert isinstance(new_curve, IncrementalCurve)


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
    assert (
        v1.value_curve.function_data.proportional_term
        == v2.value_curve.function_data.proportional_term
    )
    assert v1.value_curve.function_data.constant_term == v2.value_curve.function_data.constant_term
