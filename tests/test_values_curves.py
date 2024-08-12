from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
    PiecewiseLinearData,
    PiecewiseStepData,
    XYCoords,
)
from infrasys.value_curves import (
    InputOutputCurve,
    IncrementalCurve,
    AverageRateCurve,
    InputOutputToAverageRate,
    InputOutputToIncremental,
)
from infrasys import Component
from .models.simple_system import SimpleSystem


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
    # LinearFunctionData function data
    curve = InputOutputCurve(
        function_data=LinearFunctionData(proportional_term=1.0, constant_term=1.0)
    )
    new_curve = InputOutputToAverageRate(curve)
    assert isinstance(new_curve, AverageRateCurve)

    new_curve = InputOutputToIncremental(curve)
    assert isinstance(new_curve, IncrementalCurve)

    # Quadratic function data
    q = 3.0
    p = 2.0
    c = 1.0

    curve = InputOutputCurve(
        function_data=QuadraticFunctionData(quadratic_term=q, proportional_term=p, constant_term=c)
    )
    new_curve = InputOutputToAverageRate(curve)
    assert isinstance(new_curve, AverageRateCurve)
    assert isinstance(new_curve.function_data, LinearFunctionData)
    assert new_curve.function_data.proportional_term == q

    new_curve = InputOutputToIncremental(curve)
    assert isinstance(new_curve, IncrementalCurve)
    assert isinstance(new_curve.function_data, LinearFunctionData)
    assert new_curve.function_data.proportional_term == 2 * q

    # Piecewise linear data
    xy = [XYCoords(1.0, 2.0), XYCoords(2.0, 4.0), XYCoords(4.0, 10.0)]

    curve = InputOutputCurve(function_data=PiecewiseLinearData(points=xy))
    new_curve = InputOutputToAverageRate(curve)
    assert isinstance(new_curve, AverageRateCurve)
    assert isinstance(new_curve.function_data, PiecewiseStepData)
    assert new_curve.function_data.y_coords == [2.0, 2.5]

    new_curve = InputOutputToIncremental(curve)
    assert isinstance(new_curve, IncrementalCurve)


def test_value_curve_custom_serialization():
    component = ValueCurveComponent(
        name="test",
        value_curve=InputOutputCurve(
            function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
        ),
    )

    model_dump = component.model_dump(mode="json")
    print(model_dump)
    assert model_dump["value_curve"]["function_data"]["proportional_term"] == 1.0

    model_dump = component.model_dump(context={"magnitude_only": True})
    # print(model_dump)
    assert model_dump["value_curve"]["function_data"]["proportional_term"] == 1.0

    model_dump = component.model_dump(mode="json", context={"magnitude_only": True})
    # print(model_dump)
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
