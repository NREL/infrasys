from infrasys.function_data import PiecewiseStepData, PiecewiseLinearData, XYCoords
import pytest


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
    test_x = [2]
    test_y = [1]

    with pytest.raises(ValueError):
        PiecewiseStepData(x_coords=test_x, y_coords=test_y)

    # Check ascending x values
    test_x = [1, 4, 3]
    test_y = [2, 4]

    with pytest.raises(ValueError):
        PiecewiseStepData(x_coords=test_x, y_coords=test_y)

    # Check length of x and y lists
    test_x = [1, 2, 3]
    test_y = [2, 4, 3]

    with pytest.raises(ValueError):
        PiecewiseStepData(x_coords=test_x, y_coords=test_y)
