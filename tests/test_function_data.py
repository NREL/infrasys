from infrasys.function_data import PiecewiseLinearData, XY_COORDS
import pytest


def test_piecewise_linear():
    # Check validation for minimum x values
    test_coords = [XY_COORDS(1.0, 2.0)]

    with pytest.raises(ValueError):
        PiecewiseLinearData(points=test_coords)

    # Check validation for ascending x values
    test_coords = [XY_COORDS(1.0, 2.0), XY_COORDS(4.0, 3.0), XY_COORDS(3.0, 4.0)]

    with pytest.raises(ValueError):
        PiecewiseLinearData(points=test_coords)


# def test_piecewise_step():
#
#    return
