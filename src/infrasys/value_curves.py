"""Defines classes for value curves using cost functions"""

from infrasys import Component
from typing import Union
from typing_extensions import Annotated
from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
    PiecewiseLinearData,
    PiecewiseStepData,
    get_slopes,
)
from pydantic import Field


class InputOutputCurve(Component):
    """Input-output curve relating production quality to cost.

    An input-output curve, directly relating the production quantity to the cost: `y = f(x)`.
    Can be used, for instance, in the representation of a Cost Curve where `x` is MW
    and `y` is currency/hr, or in the representation of a Fuel Curve where `x` is MW
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
    cost: `y = f'(x)`. Can be used, for instance, in the representation of a Cost Curve
    where `x` is MW and `y` is currency/MWh, or in the representation of a Fuel Curve
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
    Cost Curve where `x` is MW and `y` is currency/MWh, or in the representation of a
    Fuel Curve where `x` is MW and `y` is fuel/MWh. Typically calculated by dividing
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


# Converting IO curves to X
def InputOutputLinearToQuadratic(data: InputOutputCurve) -> InputOutputCurve:
    """Function to convert linear InputOutput Curve to quadratic

    Parameters
    ----------
    data : InputOutputCurve
        `InputOutputCurve` using `LinearFunctionData` for its function data.

    Returns
    ----------
    InputOutputCurve
        `InputOutputCurve` using `QuadraticFunctionData` for its function data.
    """
    q = 0.0
    p = data.function_data.proportional_term
    c = data.function_data.constant_term

    return InputOutputCurve(
        function_data=QuadraticFunctionData(
            quadratic_term=q, proportional_term=p, constant_term=c
        ),
        input_at_zero=data.input_at_zero,
    )


def InputOutputToIncremental(data: InputOutputCurve) -> IncrementalCurve:
    """Function to convert InputOutputCurve to IncrementalCurve

    TEXT HERE

    Parameters
    ----------
    data : InputOutputCurve
        `InputOutputCurve` using `LinearFunctionData` for its function data.

    Returns
    ----------
    IncrementalCurve
        `IncrementalCurve` using either `LinearFunctionData` or `PiecewiseStepData`.
    """

    if isinstance(data.function_data, LinearFunctionData):
        q = 0.0
        p = data.function_data.proportional_term

        return IncrementalCurve(
            function_data=LinearFunctionData(proportional_term=q, constant_term=p),
            initial_input=data.function_data.constant_term,
            input_at_zero=data.input_at_zero,
        )
    elif isinstance(data.function_data, QuadraticFunctionData):
        q = data.function_data.quadratic_term
        p = data.function_data.proportional_term

        return IncrementalCurve(
            function_data=LinearFunctionData(proportional_term=2 * q, constant_term=p),
            initial_input=data.function_data.constant_term,
            input_at_zero=data.input_at_zero,
        )
    elif isinstance(data.function_data, PiecewiseLinearData):
        x = [fd.x for fd in data.function_data.points]
        slopes = get_slopes(data.function_data.points)

        return IncrementalCurve(
            function_data=PiecewiseStepData(x_coords=x, y_coords=slopes),
            initial_input=data.function_data.points[0].y,
            input_at_zero=data.input_at_zero,
        )

    return


def InputOutputToAverageRate(data: InputOutputCurve) -> AverageRateCurve:
    """Function to convert InputOutputCurve to AverageRateCurve

    TEXT HERE

    Parameters
    ----------
    data : InputOutputCurve
        `InputOutputCurve` using `LinearFunctionData` for its function data.

    Returns
    ----------
    AverageRateCurve
        `AverageRateCurve` using either `LinearFunctionData` or `PiecewiseStepData`.
    """
    if isinstance(data.function_data, LinearFunctionData):
        q = 0.0
        p = data.function_data.proportional_term

        return AverageRateCurve(
            function_data=LinearFunctionData(q, p),
            initial_input=data.function_data.constant_term,
            input_at_zero=data.input_at_zero,
        )
    elif isinstance(data.function_data, QuadraticFunctionData):
        q = data.function_data.quadratic_term
        p = data.function_data.proportional_term

        return AverageRateCurve(
            function_data=LinearFunctionData(q, p),
            initial_input=data.function_data.constant_term,
            input_at_zero=data.input_at_zero,
        )
    elif isinstance(data.function_data, PiecewiseLinearData):
        # I think I need to add in the getters and stuff from function_data to make this easier
        x = [fd.x for fd in data.function_data.points]
        slopes_from_origin = [fd.y / fd.x for fd in data.function_data.points[1:]]

        return AverageRateCurve(
            function_data=PiecewiseStepData(x_coords=x, y_coords=slopes_from_origin),
            initial_input=data.function_data.points[0].y,
            input_at_zero=data.input_at_zero,
        )

    return
