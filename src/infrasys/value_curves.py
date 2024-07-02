"""Defines classes for value curves using cost functions"""

from infrasys import Component
from typing import Union
from typing_extensions import Annotated
from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
    PiecewiseLinearData,
    PiecewiseStepData,
)
from pydantic import Field


class InputOutputCurve(Component):
    """Input-output curve relating production quality to cost.

    An input-output curve, directly relating the production quantity to the cost: `y = f(x)`.
    Can be used, for instance, in the representation of a [`CostCurve`](@ref) where `x` is MW
    and `y` is currency/hr, or in the representation of a [`FuelCurve`](@ref) where `x` is MW
    and `y` is fuel/hr.
    """

    name: Annotated[str, Field(frozen=True)] = ""
    function_data: Annotated[
        Union[QuadraticFunctionData, LinearFunctionData, PiecewiseLinearData],
        Field(description="The underlying `FunctionData` representation of this `ValueCurve`"),
    ]
    input_at_zero: Annotated[
        Union[None, float],
        Field(
            description="Optional, an explicit representation of the input value at zero output."
        ),
    ] = None


class IncrementalCurve(Component):
    """Incremental/marginal curve to relate production quantity to cost derivative.

    An incremental (or 'marginal') curve, relating the production quantity to the derivative of
    cost: `y = f'(x)`. Can be used, for instance, in the representation of a [`CostCurve`](@ref)
    where `x` is MW and `y` is currency/MWh, or in the representation of a [`FuelCurve`](@ref)
    where `x` is MW and `y` is fuel/MWh.
    """

    name: Annotated[str, Field(frozen=True)] = ""
    function_data: Annotated[
        Union[LinearFunctionData, PiecewiseStepData],
        Field(description="The underlying `FunctionData` representation of this `ValueCurve`"),
    ]
    initial_input: Annotated[
        Union[float, None],
        Field(
            description="The value of f(x) at the least x for which the function is defined, or \
                the origin for functions with no left endpoint, used for conversion to `InputOutputCurve`"
        ),
    ]
    input_at_zero: Annotated[
        Union[None, float],
        Field(
            description="Optional, an explicit representation of the input value at zero output."
        ),
    ] = None


class AverageRateCurve(Component):
    """Average rate curve relating production quality to average cost rate.

    An average rate curve, relating the production quantity to the average cost rate from the
    origin: `y = f(x)/x`. Can be used, for instance, in the representation of a
    [`CostCurve`](@ref) where `x` is MW and `y` is currency/MWh, or in the representation of a
    [`FuelCurve`](@ref) where `x` is MW and `y` is fuel/MWh. Typically calculated by dividing
    absolute values of cost rate or fuel input rate by absolute values of electric power.
    """

    name: Annotated[str, Field(frozen=True)] = ""
    function_data: Annotated[
        Union[LinearFunctionData, PiecewiseStepData],
        Field(
            description="The underlying `FunctionData` representation of this `ValueCurve`, or \
                only the oblique asymptote when using `LinearFunctionData`"
        ),
    ]
    initial_input: Annotated[
        Union[float, None],
        Field(
            description="The value of f(x) at the least x for which the function is defined, or \
                the origin for functions with no left endpoint, used for conversion to `InputOutputCurve`"
        ),
    ]
    input_at_zero: Annotated[
        Union[None, float],
        Field(
            description="Optional, an explicit representation of the input value at zero output."
        ),
    ] = None
