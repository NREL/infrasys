"""This module contains base class for handling pint quantity."""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from __main__ import BaseQuantity

import numpy as np
import pint
from pydantic import GetCoreSchemaHandler, SerializationInfo
from pydantic_core import core_schema

ureg = pint.UnitRegistry()


class BaseQuantity(ureg.Quantity):  # type: ignore
    """Interface for base quantity."""

    __base_unit__ = None

    def __init_subclass__(cls, **kwargs):
        if not cls.__base_unit__:
            raise TypeError("__base_unit__ should be defined")
        super().__init_subclass__(**kwargs)

    # Required for pydantic validation
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.with_info_after_validator_function(
            cls._validate,
            core_schema.union_schema(
                [
                    handler(pint.Quantity),
                    core_schema.float_schema(),
                ]
            ),
            field_name=handler.field_name,
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls._serialize, info_arg=True, return_schema=core_schema.str_schema()
            ),
        )

    # Required for pydantic validation
    @classmethod
    def _validate(cls, field_value: Any, _: core_schema.ValidationInfo) -> "BaseQuantity":
        if isinstance(field_value, BaseQuantity):
            if cls.__base_unit__:
                assert field_value.check(
                    cls.__base_unit__
                ), f"Unit must be compatible with {cls.__base_unit__}"
                return field_value
        if isinstance(field_value, pint.Quantity):
            if cls.__base_unit__:
                assert field_value.check(
                    cls.__base_unit__
                ), f"Unit must be compatible with {cls.__base_unit__}"
                return cls(field_value.magnitude, field_value.units)
            else:
                raise ValueError(f"Invalid type for BaseQuantity: {type(field_value)}")
        if isinstance(field_value, cls):
            return field_value
        if isinstance(field_value, float) or isinstance(field_value, int):
            return cls(field_value, cls.__base_unit__)
        raise TypeError("Type not supported")

    @classmethod
    def _serialize(cls, input_value, info: SerializationInfo):
        if context := info.context:
            # We can add more logic that will change the serialization here.
            magnitude_only = context.get("magnitude_only")
            if magnitude_only:
                return str(input_value.magnitude)
        return str(input_value)

    def to_dict(self) -> dict[str, Any]:
        """Convert a quantity to a dictionary for serialization."""
        val = self.magnitude
        if isinstance(self.magnitude, np.ndarray):
            val = self.magnitude.tolist()
        return {"value": val, "units": str(self.units)}

    @classmethod
    def from_dict(cls, data: dict) -> "BaseQuantity":
        """Construct the quantity from a serialized dictionary."""
        return cls(data["value"], data["units"])
