from typing_extensions import Annotated
from infrasys.component import Component
from pydantic import Field
from infrasys.value_curves import InputOutputCurve, IncrementalCurve, AverageRateCurve
from infrasys.function_data import LinearFunctionData


class ProductionVariableCostCurve(Component):
    name: Annotated[str, Field(frozen=True)] = ""


class CostCurve(ProductionVariableCostCurve):
    """Direct representation of the variable operation cost of a power plant in currency.

    Composed of a Value Curve that may represent input-output, incremental, or average rate
    data. The default units for the x-axis are MW and can be specified with
    `power_units`.
    """

    value_curve: Annotated[
        InputOutputCurve | IncrementalCurve | AverageRateCurve,
        Field(
            description="The underlying `ValueCurve` representation of this `ProductionVariableCostCurve`"
        ),
    ]
    vom_units: Annotated[
        InputOutputCurve,
        Field(description="(default: natural units (MW)) The units for the x-axis of the curve"),
    ] = InputOutputCurve(
        function_data=LinearFunctionData(proportional_term=0.0, constant_term=0.0)
    )


class FuelCurve(ProductionVariableCostCurve):
    """Representation of the variable operation cost of a power plant in terms of fuel.

    Fuel units (MBTU, liters, m^3, etc.) coupled with a conversion factor between fuel and currency.
    Composed of a Value Curve that may represent input-output, incremental, or average rate data.
    The default units for the x-axis are MW and can be specified with `power_units`.
    """

    value_curve: Annotated[
        InputOutputCurve | IncrementalCurve | AverageRateCurve,
        Field(
            description="The underlying `ValueCurve` representation of this `ProductionVariableCostCurve`"
        ),
    ]
    vom_units: Annotated[
        InputOutputCurve,
        Field(description="(default: natural units (MW)) The units for the x-axis of the curve"),
    ] = InputOutputCurve(
        function_data=LinearFunctionData(proportional_term=0.0, constant_term=0.0)
    )
    fuel_cost: Annotated[
        float,
        Field(
            description="Either a fixed value for fuel cost or the key to a fuel cost time series"
        ),
    ]
