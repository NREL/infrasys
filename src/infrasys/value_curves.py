"""Defines classes for value curves using cost functions"""

from typing_extensions import Annotated
from infrasys.component import Component
from infrasys.function_data import (
    FunctionData,
    LinearFunctionData,
    PiecewiseStepData,
)
from pydantic import Field


class ValueCurve(Component):
    name: Annotated[str, Field(frozen=True)] = ""
    input_at_zero: Annotated[
        float | None,
        Field(
            description="Optional, an explicit representation of the input value at zero output."
        ),
    ] = None


class InputOutputCurve(ValueCurve):
    """Input-output curve relating production quality to cost.

    An input-output curve, directly relating the production quantity to the cost:

    .. math:: y = f(x).

    Can be used, for instance, in the representation of a Cost Curve where :math:`x` is MW and
    :math:`y` is currency/hr, or in the representation of a Fuel Curve where :math:`x` is MW and
    :math:`y` is fuel/hr.
    """

    function_data: Annotated[
        FunctionData,
        Field(description="The underlying `FunctionData` representation of this `ValueCurve`"),
    ]


class IncrementalCurve(ValueCurve):
    """Incremental/marginal curve to relate production quantity to cost derivative.

    An incremental (or 'marginal') curve, relating the production quantity to the derivative of
    cost:

    ..math:: y = f'(x).

    Can be used, for instance, in the representation of a Cost Curve
    where :math:`x` is MW and :math:`y` is currency/MWh, or in the representation of a Fuel Curve
    where :math:`x` is MW and :math:`y` is fuel/MWh.
    """

    function_data: Annotated[
        LinearFunctionData | PiecewiseStepData,
        Field(description="The underlying `FunctionData` representation of this `ValueCurve`"),
    ]
    initial_input: Annotated[
        float | None,
        Field(
            description="The value of f(x) at the least x for which the function is defined, or \
                the origin for functions with no left endpoint, used for conversion to `InputOutputCurve`"
        ),
    ]


class AverageRateCurve(ValueCurve):
    """Average rate curve relating production quality to average cost rate.

    An average rate curve, relating the production quantity to the average cost rate from the
    origin:

    .. math:: y = f(x)/x.

    Can be used, for instance, in the representation of a
    Cost Curve where :math:`x` is MW and :math:`y` is currency/MWh, or in the representation of a
    Fuel Curve where :math:`x` is MW and :math:`y` is fuel/MWh. Typically calculated by dividing
    absolute values of cost rate or fuel input rate by absolute values of electric power.
    """

    function_data: Annotated[
        LinearFunctionData | PiecewiseStepData,
        Field(
            description="The underlying `FunctionData` representation of this `ValueCurve`, or \
                only the oblique asymptote when using `LinearFunctionData`"
        ),
    ]
    initial_input: Annotated[
        float | None,
        Field(
            description="The value of f(x) at the least x for which the function is defined, or \
                the origin for functions with no left endpoint, used for conversion to `InputOutputCurve`"
        ),
    ]
