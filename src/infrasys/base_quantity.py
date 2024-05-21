"""This module contains base class for handling pint quantity."""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from __main__ import BaseQuantity

import numpy as np
import pint
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema
from typing_extensions import Annotated

ureg = pint.UnitRegistry()


class BaseQuantity(ureg.Quantity):  # type: ignore
    """Interface for base quantity."""

    __base_unit__ = None

    def __init_subclass__(cls, **kwargs) -> None:
        if not cls.__base_unit__:
            raise TypeError("__base_unit__ should be defined")
        super().__init_subclass__(**kwargs)

    # NOTE: This creates a type hint for the unit.
    def __class_getitem__(cls):
        return Annotated.__class_getitem__((cls, cls.__base_unit__))  # type: ignore

    # Required for pydantic validation
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.with_info_after_validator_function(
            cls.validate,
            handler(pint.Quantity),
            field_name=handler.field_name,
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls.serialize, info_arg=False, return_schema=core_schema.str_schema()
            ),
        )

    # Required for pydantic validation
    @classmethod
    def validate(cls, value, *_):
        if isinstance(value, BaseQuantity):
            if cls.__base_unit__:
                assert value.check(
                    cls.__base_unit__
                ), f"Unit must be compatible with {cls.__base_unit__}"
                return value
        if isinstance(value, pint.Quantity):
            if cls.__base_unit__:
                assert value.check(
                    cls.__base_unit__
                ), f"Unit must be compatible with {cls.__base_unit__}"
                return value
            else:
                raise ValueError(f"Invalid type for BaseQuantity: {type(value)}")
        if isinstance(value, cls):
            return value
        return value

    @staticmethod
    def serialize(value):
        return str(value)

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
