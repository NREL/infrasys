from infrasys.function_data import (
    LinearFunctionData,
    PiecewiseStepData,
    PiecewiseLinearData,
    XYCoords,
    get_slopes,
)
from infrasys import Component
from .models.simple_system import SimpleSystem
import pytest


class FunctionDataComponent(Component):
    function_data: LinearFunctionData


def test_xycoords():
    test_xy = XYCoords(x=1.0, y=2.0)

    # Checking associated types
    assert isinstance(test_xy, XYCoords)

    assert isinstance(test_xy.x, float)

    assert isinstance(test_xy.y, float)


def test_piecewise_linear():
    # Check validation for minimum x values
    test_coords = [XYCoords(1.0, 2.0)]

    with pytest.raises(ValueError):
        PiecewiseLinearData(points=test_coords)

    # Check validation for ascending x values
    test_coords = [XYCoords(1.0, 2.0), XYCoords(4.0, 3.0), XYCoords(3.0, 4.0)]

    with pytest.raises(ValueError):
        PiecewiseLinearData(points=test_coords)


def test_piecewise_step():
    # Check minimum x values
    test_x = [2.0]
    test_y = [1.0]

    with pytest.raises(ValueError):
        PiecewiseStepData(x_coords=test_x, y_coords=test_y)

    # Check ascending x values
    test_x = [1.0, 4.0, 3.0]
    test_y = [2.0, 4.0]

    with pytest.raises(ValueError):
        PiecewiseStepData(x_coords=test_x, y_coords=test_y)

    # Check length of x and y lists
    test_x = [1.0, 2.0, 3.0]
    test_y = [2.0, 4.0, 3.0]

    with pytest.raises(ValueError):
        PiecewiseStepData(x_coords=test_x, y_coords=test_y)


def test_function_data_custom_serialization():
    component = FunctionDataComponent(
        name="test", function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
    )

    model_dump = component.model_dump(mode="json")
    assert model_dump["function_data"]["proportional_term"] == 1.0

    model_dump = component.model_dump(context={"magnitude_only": True})
    assert model_dump["function_data"]["proportional_term"] == 1.0

    model_dump = component.model_dump(mode="json", context={"magnitude_only": True})
    assert model_dump["function_data"]["proportional_term"] == 1.0


def test_function_data_serialization(tmp_path):
    system = SimpleSystem(auto_add_composed_components=True)

    f1 = FunctionDataComponent(
        name="test", function_data=LinearFunctionData(proportional_term=1.0, constant_term=2.0)
    )
    system.add_component(f1)
    filename = tmp_path / "function_data.json"

    system.to_json(filename, overwrite=True)
    system2 = SimpleSystem.from_json(filename)

    assert system2 is not None

    f2 = system2.get_component(FunctionDataComponent, "test")

    assert f2 is not None
    assert f1.function_data.proportional_term == f2.function_data.proportional_term
    assert f1.function_data.constant_term == f2.function_data.constant_term


def test_slopes_calculation():
    test_xy = [XYCoords(1.0, 2.0), XYCoords(2.0, 4.0), XYCoords(4.0, 10.0)]

    slopes = get_slopes(test_xy)

    correct_slopes = [2.0, 3.0]

    assert slopes == correct_slopes