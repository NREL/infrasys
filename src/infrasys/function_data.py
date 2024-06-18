from typing_extensions import Annotated
from pydantic import Field
from typing import Tuple, NamedTuple
from collections import OrderedDict
from datetime import datetime

import numpy as np

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

def get_proportional_term(fd: LinearFunctionData):
    return fd.proportional_term

def get_constant_term(fd: LinearFunctionData):
    return fd.constant_term

def _transform_linear_vector_for_hdf(data:list[LinearFunctionData]) -> list[Tuple[float, float]]:    
    transfd_data = [(get_proportional_term(fd), get_constant_term(fd)) for fd in data]
    return transfd_data

def transform_array_for_hdf(data:list[LinearFunctionData]) -> list[Tuple[float, float]]:
    return _transform_linear_vector_for_hdf(data)

def transform_array_for_hdf(data:OrderedDict[datetime, list[LinearFunctionData]]) -> list[Tuple[float, float]]:
    transfd_data = OrderedDict()
    for (k, fd) in data.items():
        transfd_data[k] = _transform_linear_vector_for_hdf(fd)
    return list(transfd_data.values())

def show(io, fd:LinearFunctionData):
    compact = getattr(io, 'compact', False)
    if not compact:
        print(f"{type(fd)} representing function ", end='', file=io)
    print(f"f(x) = {fd.proportional_term} x + {fd.constant_term}", file=io)

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

def get_quadratic_term(fd: QuadraticFunctionData):
    return fd.quadratic_term

def get_proportional_term(fd: QuadraticFunctionData):
    return fd.proportional_term

def get_constant_term(fd: QuadraticFunctionData):
    return fd.constant_term

def _transform_quadratic_vector_for_hdf(data:list[QuadraticFunctionData])  -> list[Tuple[float, float, float]]:
    transfd_data = [(get_quadratic_term(fd), get_proportional_term(fd), get_constant_term(fd)) for fd in data]
    return transfd_data

def transform_array_for_hdf(data:list[QuadraticFunctionData]) -> list[Tuple[float, float, float]]:
    return _transform_quadratic_vector_for_hdf(data)

def transform_array_for_hdf(data:OrderedDict[datetime, list[QuadraticFunctionData]]) -> list[Tuple[float, float, float]]:
    transfd_data = OrderedDict()
    for (k, fd) in data.items():
        transfd_data[k] = _transform_quadratic_vector_for_hdf(fd)
    return list(transfd_data.values())

def _validate_piecewise_x(x_coords: list):
    if len(x_coords) < 2:
        raise ValueError("Must specify at least two x-coordinates")
    if not (x_coords == sorted(x_coords) or (np.isnan(x_coords[0]) and x_coords[1:] == sorted(x_coords[1:]))):
        raise ValueError(f"Piecewise x-coordinates must be ascending, got {x_coords}")

def show(io, fd:QuadraticFunctionData):
    compact = getattr(io, 'compact', False)
    if not compact:
        print(f"{type(fd)} representing function ", end='', file=io)
    print(f"f(x) = {fd.quadratic_term} x^2 + {fd.proportional_term} x + {fd.constant_term}", file=io)