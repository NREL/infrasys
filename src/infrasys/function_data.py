from infrasys import Component
from typing_extensions import Annotated
from pydantic import Field
from pydantic.functional_validators import AfterValidators
from typing import NamedTuple
import numpy as np


class XY_COORDS(NamedTuple):
    x: float
    y: float


class LinearFunctionData(Component):
    """
    Class to represent the underlying data of linear functions. Principally used for
    the representation of cost functions `f(x) = proportional_term*x + constant_term`.

    """

    name: Annotated[str, Field(frozen=True)] = ""
    # match above annotation
    proportional_term: Annotated[
        float, Field(description="the proportional term in the represented function")
    ]
    constant_term: Annotated[
        float, Field(description="the constant term in the represented function")
    ]


class QuadraticFunctionData(Component):
    """
    Class to represent the underlying data of quadratic functions. Principally used for the
    representation of cost functions
    `f(x) = quadratic_term*x^2 + proportional_term*x + constant_term`.
    """

    name: Annotated[str, Field(frozen=True)] = ""
    quadratic_term: Annotated[
        float, Field(description="the quadratic term in the represented function")
    ]
    proportional_term: Annotated[
        float, Field(description="the proportional term in the represented function")
    ]
    constant_term: Annotated[
        float, Field(description="the constant term in the represented function")
    ]


def validate_piecewise_x(points: list[XY_COORDS]):
    x_coords = [p.x for p in points]

    if len(x_coords) < 2:
        raise ValueError("Must specify at least two x-coordinates")
    if not (
        x_coords == sorted(x_coords)
        or (np.isnan(x_coords[0]) and x_coords[1:] == sorted(x_coords[1:]))
    ):
        raise ValueError(f"Piecewise x-coordinates must be ascending, got {x_coords}")

    return points


class PiecewiseLinearData(Component):
    r"""
    Class to represent piecewise linear data as a series of points: two points define one
    segment, three points define two segments, etc. The curve starts at the first point given,
    not the origin. Principally used for the representation of cost functions where the points
    store quantities (x, y), such as (MW, \$/h).
    """

    name: Annotated[str, Field(frozen=True)] = ""
    points: Annotated[
        list[XY_COORDS],
        AfterValidators(validate_piecewise_x),
        Field(description="list of (x,y) points that define the function"),
    ]

    # Decision to keep as named tuple -> Put into PR
    # Change validate function to match the NamedTuple


class PiecewiseStepData(Component):
    r"""
    Class to represent a step function as a series of endpoint x-coordinates and segment
    y-coordinates: two x-coordinates and one y-coordinate defines a single segment, three
    x-coordinates and two y-coordinates define two segments, etc. This can be useful to
    represent the derivative of a [PiecewiseLinearData](@ref), where the y-coordinates of this
    step function represent the slopes of that piecewise linear function, so there is also an
    optional field `c` that can be used to store the initial y-value of that piecewise linear
    function. Principally used for the representation of cost functions where the points store
    quantities (x, dy/dx), such as (MW, \$/MWh).
    """

    name: Annotated[str, Field(frozen=True)] = ""
    x_coords: Annotated[
        list[float], Field(description="the x-coordinates of the endpoints of the segments")
    ]
    y_coords: Annotated[
        list[float],
        Field(
            description="the y-coordinates of the segments: `y_coords[1]` is the y-value between `x_coords[0]` and `x_coords[1]`, etc. \
                Must have one fewer elements than `x_coords`."
        ),
    ]
