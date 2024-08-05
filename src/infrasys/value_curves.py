"""Defines classes for value curves using cost functions"""

from infrasys import Component
from typing import Union
from typing_extensions import Annotated
from infrasys.exceptions import ISMethodError
from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
    PiecewiseLinearData,
    PiecewiseStepData,
    get_slopes,
    running_sum,
)
from pydantic import Field
import numpy as np


class InputOutputCurve(Component):
    """Input-output curve relating production quality to cost.

    An input-output curve, directly relating the production quantity to the cost:

    .. math:: y = f(x).

    Can be used, for instance, in the representation of a Cost Curve where :math:`x` is MW
    and :math:`y` is currency/hr, or in the representation of a Fuel Curve where :math:`x` is MW
    and :math:`y` is fuel/hr.
    """

    name: Annotated[str, Field(frozen=True)] = ""
    function_data: Annotated[
        QuadraticFunctionData | LinearFunctionData | PiecewiseLinearData,
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
    cost:

    ..math:: y = f'(x).

    Can be used, for instance, in the representation of a Cost Curve
    where :math:`x` is MW and :math:`y` is currency/MWh, or in the representation of a Fuel Curve
    where :math:`x` is MW and :math:`y` is fuel/MWh.
    """

    name: Annotated[str, Field(frozen=True)] = ""
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
    input_at_zero: Annotated[
        float | None,
        Field(
            description="Optional, an explicit representation of the input value at zero output."
        ),
    ] = None


class AverageRateCurve(Component):
    """Average rate curve relating production quality to average cost rate.

    An average rate curve, relating the production quantity to the average cost rate from the
    origin:

    .. math:: y = f(x)/x.

    Can be used, for instance, in the representation of a
    Cost Curve where :math:`x` is MW and :math:`y` is currency/MWh, or in the representation of a
    Fuel Curve where :math:`x` is MW and :math:`y` is fuel/MWh. Typically calculated by dividing
    absolute values of cost rate or fuel input rate by absolute values of electric power.
    """

    name: Annotated[str, Field(frozen=True)] = ""
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
    input_at_zero: Annotated[
        float | None,
        Field(
            description="Optional, an explicit representation of the input value at zero output."
        ),
    ] = None


def InputOutputLinearToQuadratic(data: InputOutputCurve) -> InputOutputCurve:
    """Function to convert linear InputOutput Curve to quadratic

    Converting IO curves to X

    Parameters
    ----------
    data : InputOutputCurve
        `InputOutputCurve` using `LinearFunctionData` for its function data.

    Returns
    -------
    InputOutputCurve
        `InputOutputCurve` using `QuadraticFunctionData` for its function data.
    """
    q = 0.0

    if isinstance(data.function_data, PiecewiseStepData | PiecewiseLinearData):
        raise ISMethodError("Can not convert Piecewise data to Quadratic.")

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
    ISMethodError
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
        case _:
            raise ISMethodError("Function is not valid for the type of data provided.")


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
    ISMethodError
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
        case _:
            raise ISMethodError("Function is not valid for the type of data provided.")


def IncrementalToInputOutput(data: IncrementalCurve) -> InputOutputCurve:
    """Function to convert IncrementalCurve to InputOutputCurve

    Function takes an IncrementalCurve and converts it to a corresponding
    InputOutputCurve depending on the type of function_data. If the IncrementalCurve
    uses LinearFunctionData, the new InputOutputCurve is created linear or quadratic data
    that correspond to the integral of the original linear function. If the input uses
    PiecewiseStepData, the slopes of each segment are used to calculate the corresponding
    y values for each x value and used to construct PiecewiseLinearData for the InputOutputCurve.

    Parameters
    ----------
    data : IncrementalCurve
        Original IncrementalCurve for conversion.

    Returns
    ----------
    InputOutputCurve
        InputOutputCurve using either QuadraticFunctionData or PiecewiseStepData.
    """
    if isinstance(data.function_data, LinearFunctionData):
        p = data.function_data.proportional_term
        m = data.function_data.constant_term

        c = data.initial_input
        if c is None:
            raise ValueError("Cannot convert `IncrementalCurve` with undefined `initial_input`")

        if p == 0:
            return InputOutputCurve(
                function_data=LinearFunctionData(proportional_term=m, constant_term=c)
            )
        else:
            return InputOutputCurve(
                function_data=QuadraticFunctionData(
                    quadratic_term=p / 2, proportional_term=m, constant_term=c
                ),
                input_at_zero=data.input_at_zero,
            )

    elif isinstance(data.function_data, PiecewiseStepData):
        c = data.initial_input
        if c is None:
            raise ValueError("Cannot convert `IncrementalCurve` with undefined `initial_input`")

        points = running_sum(data.function_data)

        return InputOutputCurve(
            function_data=PiecewiseLinearData(points=[(p.x, p.y + c) for p in points]),
            input_at_zero=data.input_at_zero,
        )


def IncrementalToAverageRate(data: IncrementalCurve) -> AverageRateCurve:
    """Function to convert IncrementalCurve to AverageRateCurve

    Function takes an IncrementalCurve and first converts it to an
    InputOutputCurve, which is then converted into an AverageRateCurve.

    Parameters
    ----------
    data : IncrementalCurve
        Original InputOutputCurve for conversion.

    Returns
    ----------
    AverageRateCurve
        AverageRateCurve using either QuadraticFunctionData or PiecewiseStepData.
    """

    io_curve = IncrementalToInputOutput(data)

    return InputOutputToAverageRate(io_curve)


def AverageRateToInputOutput(data: AverageRateCurve) -> InputOutputCurve:
    """Function to convert IncrementalCurve to InputOutputCurve

    Function takes an AverageRateCurve and converts it to a corresponding
    InputOutputCurve depending on the type of function_data. If the AverageRateCurve
    uses LinearFunctionData, the new InputOutputCurve is created with either linear or quadratic
    function data, depending on if the original function data is constant or linear. If the
    input uses PiecewiseStepData, new y-values are calculated for each x value such that `f(x) = x*y`
    and used to construct PiecewiseLinearData for the InputOutputCurve.

    Parameters
    ----------
    data : AverageRateCurve
        Original AverageRateCurve for conversion.

    Returns
    ----------
    InputOutputCurve
        InputOutputCurve using either QuadraticFunctionData or PiecewiseStepData.
    """
    match data.function_data:
        case LinearFunctionData():
            p = data.function_data.proportional_term
            m = data.function_data.constant_term

            c = data.initial_input
            if c is None:
                raise ValueError(
                    "Cannot convert `AverageRateCurve` with undefined `initial_input`"
                )

            if p == 0:
                return InputOutputCurve(
                    function_data=LinearFunctionData(proportional_term=m, constant_term=c)
                )
            else:
                return InputOutputCurve(
                    function_data=QuadraticFunctionData(
                        quadratic_term=p, proportional_term=m, constant_term=c
                    ),
                    input_at_zero=data.input_at_zero,
                )
        case PiecewiseStepData():
            c = data.initial_input
            if c is None:
                raise ISMethodError(
                    "Cannot convert `AverageRateCurve` with undefined `initial_input`"
                )

            xs = data.function_data.x_coords
            ys = np.multiply(xs[1:], data.function_data.y_coords).tolist()
            ys.insert(0, c)

            return InputOutputCurve(
                function_data=PiecewiseLinearData(points=list(zip(xs, ys))),
                input_at_zero=data.input_at_zero,
            )
        case _:
            raise ISMethodError("Function is not valid for the type of data provided.")


def AverageRateToIncremental(data: AverageRateCurve) -> IncrementalCurve:
    """Function to convert AverageRateCurve to IncrementalCurve

    Function takes an AverageRateCurve and first converts it to an
    InputOutputCurve, which is then converted into an IncrementalCurve.

    Parameters
    ----------
    data : AverageRateCurve
        Original AverageRateCurve for conversion.

    Returns
    ----------
    IncrementalCurve
        IncrementalCurve using either QuadraticFunctionData or PiecewiseStepData.
    """

    io_curve = AverageRateToInputOutput(data)

    return InputOutputToIncremental(io_curve)
