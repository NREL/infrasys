"""Defines models for cost functions"""

from infrasys import Component
from typing_extensions import Annotated
from pydantic import Field, model_validator
from pydantic.functional_validators import AfterValidator
from typing import NamedTuple, List
import numpy as np


class XYCoords(NamedTuple):
    """Named tuple used to define (x,y) coordinates."""

    x: float
    y: float


class LinearFunctionData(Component):
    """Data representation for linear cost function.

    Class to represent the underlying data of linear functions. Principally used for
    the representation of cost functions `f(x) = proportional_term*x + constant_term`.
    """

    name: Annotated[str, Field(frozen=True)] = ""
    proportional_term: Annotated[
        float, Field(description="the proportional term in the represented function")
    ]
    constant_term: Annotated[
        float, Field(description="the constant term in the represented function")
    ]


class QuadraticFunctionData(Component):
    """Data representation for quadratic cost functions.

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


def validate_piecewise_linear_x(points: List[XYCoords]) -> List[XYCoords]:
    """Validates the x data for PiecewiseLinearData class

    Function used to validate given x data for the PiecewiseLinearData class.
    X data is checked to ensure there is at least two values of x,
    which is the minimum required to generate a cost curve, and is
    given in ascending order (e.g. [1, 2, 3], not [1, 3, 2]).

    Parameters
    ----------
    points : List[XYCoords]
        List of named tuples of (x,y) coordinates for cost function

    Returns
    ----------
    points : List[XYCoords]
        List of (x,y) data for cost function after successful validation.
    """

    x_coords = [p.x for p in points]

    if len(x_coords) < 2:
        raise ValueError("Must specify at least two x-coordinates")
    if not (
        x_coords == sorted(x_coords)
        or (np.isnan(x_coords[0]) and x_coords[1:] == sorted(x_coords[1:]))
    ):
        raise ValueError(f"Piecewise x-coordinates must be ascending, got {x_coords}")

    return points


def validate_piecewise_step_x(x_coords: List[float]) -> List[float]:
    """Validates the x data for PiecewiseStepData class

    Function used to validate given x data for the PiecewiseStepData class.
    X data is checked to ensure there is at least two values of x,
    which is the minimum required to generate a cost curve, and is
    given in ascending order (e.g. [1, 2, 3], not [1, 3, 2]).

    Parameters
    ----------
    x_coords : List[float]
        List of x data for cost function.

    Returns
    ----------
    x_coords : List[float]
        List of x data for cost function after successful validation.
    """

    if len(x_coords) < 2:
        raise ValueError("Must specify at least two x-coordinates")
    if not (
        x_coords == sorted(x_coords)
        or (np.isnan(x_coords[0]) and x_coords[1:] == sorted(x_coords[1:]))
    ):
        raise ValueError(f"Piecewise x-coordinates must be ascending, got {x_coords}")

    return x_coords


class PiecewiseLinearData(Component):
    """Data representation for piecewise linear cost function.

    Class to represent piecewise linear data as a series of points: two points define one
    segment, three points define two segments, etc. The curve starts at the first point given,
    not the origin. Principally used for the representation of cost functions where the points
    store quantities (x, y), such as (MW, USD/h).
    """

    name: Annotated[str, Field(frozen=True)] = ""
    points: Annotated[
        List[XYCoords],
        AfterValidator(validate_piecewise_linear_x),
        Field(description="list of (x,y) points that define the function"),
    ]


class PiecewiseStepData(Component):
    """Data representation for piecewise step cost function.

    Class to represent a step function as a series of endpoint x-coordinates and segment
    y-coordinates: two x-coordinates and one y-coordinate defines a single segment, three
    x-coordinates and two y-coordinates define two segments, etc. This can be useful to
    represent the derivative of a `PiecewiseLinearData`, where the y-coordinates of this
    step function represent the slopes of that piecewise linear function.
    Principally used for the representation of cost functions where the points store
    quantities (x, dy/dx), such as (MW, USD/MWh).
    """

    name: Annotated[str, Field(frozen=True)] = ""
    x_coords: Annotated[
        List[float],
        Field(description="the x-coordinates of the endpoints of the segments"),
    ]
    y_coords: Annotated[
        List[float],
        Field(
            description="the y-coordinates of the segments: `y_coords[1]` is the y-value \
                between `x_coords[0]` and `x_coords[1]`, etc. Must have one fewer elements than `x_coords`."
        ),
    ]

    @model_validator(mode="after")
    def validate_piecewise_xy(self):
        """Method to validate the x and y data for PiecewiseStepData class

        Model validator used to validate given data for the PiecewiseStepData class.
        Calls `validate_piecewise_step_x` to check if `x_coords` is valid, then checks if
        the length of `y_coords` is exactly one less than `x_coords`, which is necessary
        to define the cost functions correctly.
        """
        validate_piecewise_step_x(self.x_coords)

        if len(self.y_coords) != len(self.x_coords) - 1:
            raise ValueError("Must specify one fewer y-coordinates than x-coordinates")

        return self


def get_slopes(vc: List[XYCoords]) -> List[float]:
    """Calculate slopes from XYCoord data

    Function used to calculate the slopes from a list of XYCoords.
    Slopes are calculated between each section of the piecewise curve.
    Returns a list of slopes that can be used to define Value Curves.

    Parameters
    ----------
    vc : List[XYCoords]
        List of named tuples of (x,y) coordinates.

    Returns
    ----------
    slopes : List[float]
        List of slopes for each section of given piecewise linear data.
    """
    slopes = []
    (prev_x, prev_y) = vc[0]
    for comp_x, comp_y in vc[1:]:
        slopes.append((comp_y - prev_y) / (comp_x - prev_x))
        (prev_x, prev_y) = (comp_x, comp_y)
    return slopes


def get_x_lengths(x_coords: List[float]) -> List[float]:
    return np.subtract(x_coords[1:], x_coords[:-1]).tolist()


def running_sum(data: PiecewiseStepData) -> List[XYCoords]:
    points = []
    slopes = data.y_coords
    x_coords = data.x_coords
    x_lengths = get_x_lengths(x_coords)
    running_y = 0.0

    points.append(XYCoords(x=x_coords[0], y=running_y))
    for prev_slope, this_x, dx in zip(slopes, x_coords[1:], x_lengths):
        running_y += prev_slope * dx
        points.append(XYCoords(x=this_x, y=running_y))

    return points
