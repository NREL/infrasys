"""Normalization utilities"""

from enum import Enum

import numpy as np
from loguru import logger


class NormalizationType(str, Enum):
    MAX = "max"


def normalize_array(data: np.ndarray, normalization_type: NormalizationType) -> np.ndarray:
    """Return a normalized array with the specified algorithm."""
    match normalization_type:
        case NormalizationType.MAX:
            normalized = data / np.max(data)
        case _:
            msg = f"{normalization_type=}"
            raise NotImplementedError(msg)

    logger.debug("Normalized array with {}", normalization_type)
    return normalized
