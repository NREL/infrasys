"""Normalization utilities"""

import abc
from enum import Enum
from typing import Literal, Optional, Annotated, Union

import numpy as np
from pydantic import Field

from infrasys.models import InfraSysBaseModel


class NormalizationType(str, Enum):
    MAX = "max"
    BY_VALUE = "by_value"


class NormalizationBase(InfraSysBaseModel, abc.ABC):
    """Base class for all normalization models"""

    @abc.abstractmethod
    def normalize_array(self, data: np.ndarray) -> np.ndarray:
        """Normalize the array."""


class NormalizationMax(NormalizationBase):
    """Perform normalization by the max value in an array."""

    max_value: Optional[float] = None
    normalization_type: Literal[NormalizationType.MAX] = NormalizationType.MAX

    def normalize_array(self, data: np.ndarray) -> np.ndarray:
        self.max_value = np.max(data)
        return data / self.max_value


class NormalizationByValue(NormalizationBase):
    """Perform normalization by a user-defined value."""

    value: float
    normalization_type: Literal[NormalizationType.BY_VALUE] = NormalizationType.BY_VALUE

    def normalize_array(self, data: np.ndarray) -> np.ndarray:
        return data / self.value


NormalizationModel = Annotated[
    Union[None, NormalizationMax, NormalizationByValue],
    Field(
        description="Defines the type of normalization performed on the data, if any.",
        discriminator="normalization_type",
    ),
]
