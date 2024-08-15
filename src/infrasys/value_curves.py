"""Defines classes for value curves using cost functions"""

from typing_extensions import Annotated
from infrasys.component import Component
from infrasys.exceptions import ISOperationNotAllowed
from infrasys.function_data import (
    LinearFunctionData,
    QuadraticFunctionData,
    PiecewiseLinearData,
    PiecewiseStepData,
    running_sum,
)
from pydantic import Field
import numpy as np


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

    def to_input_output(self) -> InputOutputCurve:
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
        match self.function_data:
            case LinearFunctionData():
                p = self.function_data.proportional_term
                m = self.function_data.constant_term

                c = self.initial_input
                if c is None:
                    msg = "Cannot convert `IncrementalCurve` with undefined `initial_input`"
                    raise ISOperationNotAllowed(msg)

                if p == 0:
                    return InputOutputCurve(
                        function_data=LinearFunctionData(proportional_term=m, constant_term=c)
                    )
                else:
                    return InputOutputCurve(
                        function_data=QuadraticFunctionData(
                            quadratic_term=p / 2, proportional_term=m, constant_term=c
                        ),
                        input_at_zero=self.input_at_zero,
                    )
            case PiecewiseStepData():
                c = self.initial_input
                if c is None:
                    msg = "Cannot convert `IncrementalCurve` with undefined `initial_input`"
                    raise ISOperationNotAllowed(msg)

                points = running_sum(self.function_data)

                return InputOutputCurve(
                    function_data=PiecewiseLinearData(points=[(p.x, p.y + c) for p in points]),
                    input_at_zero=self.input_at_zero,
                )


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

    def to_input_output(self) -> InputOutputCurve:
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
        match self.function_data:
            case LinearFunctionData():
                p = self.function_data.proportional_term
                m = self.function_data.constant_term

                c = self.initial_input
                if c is None:
                    msg = "Cannot convert `AverageRateCurve` with undefined `initial_input`"
                    raise ISOperationNotAllowed(msg)

                if p == 0:
                    return InputOutputCurve(
                        function_data=LinearFunctionData(proportional_term=m, constant_term=c)
                    )
                else:
                    return InputOutputCurve(
                        function_data=QuadraticFunctionData(
                            quadratic_term=p, proportional_term=m, constant_term=c
                        ),
                        input_at_zero=self.input_at_zero,
                    )
            case PiecewiseStepData():
                c = self.initial_input
                if c is None:
                    msg = "Cannot convert `AverageRateCurve` with undefined `initial_input`"
                    raise ISOperationNotAllowed(msg)

                xs = self.function_data.x_coords
                ys = np.multiply(xs[1:], self.function_data.y_coords).tolist()
                ys.insert(0, c)

                return InputOutputCurve(
                    function_data=PiecewiseLinearData(points=list(zip(xs, ys))),
                    input_at_zero=self.input_at_zero,
                )
            case _:
                msg = "Function is not valid for the type of data provided."
                raise ISOperationNotAllowed(msg)
