from typing_extensions import Annotated
from pydantic import Field
from typing import Tuple, NamedTuple
from collections import OrderedDict
from datetime import datetime

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

def get_proportional_term(fd: LinearFunctionData) -> float:
    return fd.proportional_term

def get_constant_term(fd: LinearFunctionData) -> float:
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
