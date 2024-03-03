"""Normalization utilities"""

from enum import Enum

import numpy as np
from loguru import logger


class NormalizationType(str, Enum):
    MAX = "max"
    MIN_MAX = "min_max"


def normalize_array(data: np.ndarray, normalization_type: NormalizationType) -> np.ndarray:
    """Return a normalized array with the specified algorithm."""
    match normalization_type:
        case NormalizationType.MAX:
            normalized = data / np.max(data)
        case NormalizationType.MIN_MAX:
            min_val = np.min(data)
            max_val = np.max(data)
            normalized = (data - min_val) / (max_val - min_val)
        case _:
            msg = f"{normalization_type=}"
            raise NotImplementedError(msg)

    logger.debug("Normalized array with {}", normalization_type)
    return normalized
