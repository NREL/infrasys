from typing_extensions import Annotated
from infrasys.component import Component
from pydantic import Field
from infrasys.value_curves import InputOutputCurve, IncrementalCurve, AverageRateCurve
from infrasys.function_data import LinearFunctionData


class ProductionVariableCostCurve(Component):
    name: Annotated[str, Field(frozen=True)] = ""
    value_curve: Annotated[
        InputOutputCurve | IncrementalCurve | AverageRateCurve,
        Field(
            description="The underlying `ValueCurve` representation of this `ProductionVariableCostCurve`"
        ),
    ]
    # not float change this later, should be UnitSystem but not sure if thats in infrasys?
    power_units: Annotated[
        float,
        Field(description="(default: natural units (MW)) The units for the x-axis of the curve"),
    ]
    vom_units: Annotated[
        InputOutputCurve,
        Field(description="(default: natural units (MW)) The units for the x-axis of the curve"),
    ] = InputOutputCurve(LinearFunctionData(0.0))


class CostCurve(ProductionVariableCostCurve):
    x = 1


class FuelCurve(ProductionVariableCostCurve):
    fuel_cost: Annotated[
        float,
        Field(
            description="Either a fixed value for fuel cost or the key to a fuel cost time series"
        ),
    ]
