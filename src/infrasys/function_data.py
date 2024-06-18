from typing_extensions import Annotated
from pydantic import Field
from typing import NamedTuple

import numpy as np

class XY_COORDS(NamedTuple):
    x:float
    y:float

"""
Class to represent the underlying data of linear functions. Principally used for
the representation of cost functions `f(x) = proportional_term*x + constant_term`.

# Arguments
 - `proportional_term:float`: the proportional term in the represented function
 - `constant_term:float`: the constant term in the represented function
"""
class LinearFunctionData:

    name: Annotated[str, Field(frozen=True)] = ""
    proportional_term: float
    constant_term: float

"""
Class to represent the underlying data of quadratic functions. Principally used for the
representation of cost functions
`f(x) = quadratic_term*x^2 + proportional_term*x + constant_term`.

# Arguments
 - `quadratic_term:float`: the quadratic term in the represented function
 - `proportional_term:float`: the proportional term in the represented function
 - `constant_term:float`: the constant term in the represented function
"""

class QuadraticFunctionData:

    name: Annotated[str, Field(frozen=True)] = ""
    quadratic_term: float
    proportional_term: float
    constant_term: float

def _validate_piecewise_x(x_coords: list):
    if len(x_coords) < 2:
        raise ValueError("Must specify at least two x-coordinates")
    if not (x_coords == sorted(x_coords) or (np.isnan(x_coords[0]) and x_coords[1:] == sorted(x_coords[1:]))):
        raise ValueError(f"Piecewise x-coordinates must be ascending, got {x_coords}")

"""
Class to represent piecewise linear data as a series of points: two points define one
segment, three points define two segments, etc. The curve starts at the first point given,
not the origin. Principally used for the representation of cost functions where the points
store quantities (x, y), such as (MW, \$/h).

# Arguments
 - `points::Vector{@NamedTuple{x::Float64, y::Float64}}`: the points that define the function
"""

class PiecewiseLinearData:
    
    name: Annotated[str, Field(frozen=True)] = ""
    #Okay this should not be a list, it should be a tuple of two lists, one for x, one for y
    points:list[XY_COORDS]

    def PiecewiseLinearData(points:list[NamedTuple]):
        _validate_piecewise_x(points[0])
        return points

"""
Class to represent a step function as a series of endpoint x-coordinates and segment
y-coordinates: two x-coordinates and one y-coordinate defines a single segment, three
x-coordinates and two y-coordinates define two segments, etc. This can be useful to
represent the derivative of a [PiecewiseLinearData](@ref), where the y-coordinates of this
step function represent the slopes of that piecewise linear function, so there is also an
optional field `c` that can be used to store the initial y-value of that piecewise linear
function. Principally used for the representation of cost functions where the points store
quantities (x, dy/dx), such as (MW, \$/MWh).

# Arguments
 - `x_coords:list[float]`: the x-coordinates of the endpoints of the segments
 - `y_coords::list[float]`: the y-coordinates of the segments: `y_coords[1]` is the y-value between
 `x_coords[1]` and `x_coords[2]`, etc. Must have one fewer elements than `x_coords`.
 - `c::Union{Nothing, Float64}`: optional, the value to use for the integral from 0 to `x_coords[1]` of this function
"""

class PiecewiseStepData:
    
    name: Annotated[str, Field(frozen=True)] = ""
    x_coords:list[float]
    y_coords:list[float]

    def PiecewiseStepData(x_coords, y_coords):
        if len(y_coords) == len(x_coords):
            # To make the lengths match for HDF serialization, we prepend NaN to y_coords
            if np.isnan(y_coords[0]):
                return PiecewiseStepData(x_coords, y_coords[1:])
            # To leave x_coords[1] undefined, must explicitly pass in NaN

        _validate_piecewise_x(x_coords)
        if len(y_coords) != len(x_coords) - 1:
            raise ValueError("Must specify one fewer y-coordinates than x-coordinates")
        
        return (x_coords, y_coords)
    