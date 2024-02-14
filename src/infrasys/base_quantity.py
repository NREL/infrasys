""" This module contains base class for handling pint quantity."""

from abc import ABC
from typing import Any

import numpy as np
import pint

ureg = pint.UnitRegistry()


class BaseQuantity(ureg.Quantity, ABC):  # type: ignore
    """Interface for base quantity."""

    def __new__(cls, value, units, **kwargs):
        instance = super().__new__(cls, value, units, **kwargs)
        if not hasattr(cls, "__compatible_unit__"):
            raise ValueError("You should define __compatible_unit__ attribute in your class.")
        if not instance.is_compatible_with(cls.__compatible_unit__):
            message = f"{__class__} must be compatible with {cls.__compatible_unit__}, not {units}"
            raise ValueError(message)
        return instance

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
