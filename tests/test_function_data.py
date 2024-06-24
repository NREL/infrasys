from infrasys.function_data import PiecewiseStepData, PiecewiseLinearData, XY_COORDS
import pytest


def test_xy_coords():
    test_xy = XY_COORDS(x=1.0, y=2.0)

    # Checking associated types
    assert isinstance(test_xy, XY_COORDS)

    assert isinstance(test_xy.x, float)

    assert isinstance(test_xy.y, float)


def test_piecewise_linear():
    # Check validation for minimum x values
    test_coords = [XY_COORDS(1.0, 2.0)]

    with pytest.raises(ValueError):
        PiecewiseLinearData(points=test_coords)

    # Check validation for ascending x values
    test_coords = [XY_COORDS(1.0, 2.0), XY_COORDS(4.0, 3.0), XY_COORDS(3.0, 4.0)]

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
