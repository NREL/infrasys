"""Defines classes for value curves using cost functions"""

from typing_extensions import Annotated
from infrasys.component import Component
from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
    PiecewiseLinearData,
    PiecewiseStepData,
    get_slopes,
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
        LinearFunctionData | QuadraticFunctionData | PiecewiseLinearData,
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


def InputOutputToIncremental(data: InputOutputCurve) -> IncrementalCurve:
    """Function to convert InputOutputCurve to IncrementalCurve

    Function takes and InputOutputCurve and converts it to a corresponding
    incremental curve depending on the type of function_data. If the :class:`InputOutputCurve`
    uses :class:`LinearFunctionData` or :class:`QuadraticFunctionData`, the corresponding
    :class:`IncrementalCurve` uses the corresponding derivative for its `function_data`. If
    the input uses :class:`PiecewiseLinearData`, the slopes of each segment are calculated and
    converted to PiecewiseStepData for the IncrementalCurve.

    Parameters
    ----------
    data : InputOutputCurve
        Original InputOutputCurve for conversion.

    Returns
    -------
    IncrementalCurve
        IncrementalCurve using either LinearFunctionData or PiecewiseStepData after conversion.

    Raises
    ------
    ISOperationNotAllowed
        Function is not valid for the type of data provided.
    """
    match data.function_data:
        case LinearFunctionData():
            q = 0.0
            p = data.function_data.proportional_term

            return IncrementalCurve(
                function_data=LinearFunctionData(proportional_term=q, constant_term=p),
                initial_input=data.function_data.constant_term,
                input_at_zero=data.input_at_zero,
            )
        case QuadraticFunctionData():
            q = data.function_data.quadratic_term
            p = data.function_data.proportional_term

            return IncrementalCurve(
                function_data=LinearFunctionData(proportional_term=2 * q, constant_term=p),
                initial_input=data.function_data.constant_term,
                input_at_zero=data.input_at_zero,
            )
        case PiecewiseLinearData():
            x = [fd.x for fd in data.function_data.points]
            slopes = get_slopes(data.function_data.points)

            return IncrementalCurve(
                function_data=PiecewiseStepData(x_coords=x, y_coords=slopes),
                initial_input=data.function_data.points[0].y,
                input_at_zero=data.input_at_zero,
            )


def InputOutputToAverageRate(data: InputOutputCurve) -> AverageRateCurve:
    """Function to convert InputOutputCurve to AverageRateCurve

    If the :class:`InputOutputCurve` uses :class:`LinearFunctionData` or
    :class:`QuadraticFunctionData`, the corresponding :class:`IncrementalCurve`
    uses the :class`LinearFunctionData`, with a slope equal to the
    `quadratic_term` (0.0 if originally linear), and a intercept equal to the
    `proportional_term`. If the input uses :class:`PiecewiseLinearData`, the
    slopes of each segment are calculated and converted to PiecewiseStepData
    for the AverageRateCurve.

    Parameters
    ----------
    data : InputOutputCurve
        Original InputOutputCurve for conversion.

    Returns
    ----------
    AverageRateCurve
        AverageRateCurve using either LinearFunctionData or PiecewiseStepData after conversion.

    Raises
    ------
    ISOperationNotAllowed
        Function is not valid for the type of data provided.
    """
    match data.function_data:
        case LinearFunctionData():
            q = 0.0
            p = data.function_data.proportional_term

            return AverageRateCurve(
                function_data=LinearFunctionData(proportional_term=q, constant_term=p),
                initial_input=data.function_data.constant_term,
                input_at_zero=data.input_at_zero,
            )
        case QuadraticFunctionData():
            q = data.function_data.quadratic_term
            p = data.function_data.proportional_term

            return AverageRateCurve(
                function_data=LinearFunctionData(proportional_term=q, constant_term=p),
                initial_input=data.function_data.constant_term,
                input_at_zero=data.input_at_zero,
            )
        case PiecewiseLinearData():
            # I think I need to add in the getters and stuff from function_data to make this easier
            x = [fd.x for fd in data.function_data.points]
            slopes_from_origin = [fd.y / fd.x for fd in data.function_data.points[1:]]

            return AverageRateCurve(
                function_data=PiecewiseStepData(x_coords=x, y_coords=slopes_from_origin),
                initial_input=data.function_data.points[0].y,
                input_at_zero=data.input_at_zero,
            )
