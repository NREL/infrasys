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


    