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
