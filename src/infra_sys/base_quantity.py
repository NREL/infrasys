""" This module contains base class for handling pint quantity."""
# standard imports
from abc import ABC

# third-party imports
import pint

# internal imports
from infra_sys.common import TYPE_INFO
from infra_sys.models import SerializedTypeInfo


class BaseQuantity(pint.Quantity, ABC):
    """Interface for base quantity."""

    def __new__(cls, value, units, **kwargs):
        instance = super().__new__(cls, value, units, **kwargs)
        if not hasattr(cls, "__compatible_unit__"):
            raise ValueError("You should define __compatible_unit__ attribute in your class.")
        if not instance.is_compatible_with(cls.__compatible_unit__):
            message = f"{__class__} must be compatible with {cls.__compatible_unit__}, not {units}"
            raise ValueError(message)
        return instance

    def to_dict(self):
        """Method to convert quantity to dict"""
        return {
            "value": self.magnitude,
            "units": str(self.units),
            TYPE_INFO: SerializedTypeInfo(
                module=self.__module__, type=self.__class__.__name__
            ).model_dump(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BaseQuantity":
        """Build from dict."""

        return cls(data["value"], data["units"])
