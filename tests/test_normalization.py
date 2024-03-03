import numpy as np

from infrasys.normalization import NormalizationType, normalize_array


def test_normalization_max():
    data = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
    result = normalize_array(data, NormalizationType.MAX)
    for i, x in enumerate(result):
        assert x == data[i] / 5.5


def test_normalization_min_max():
    data = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
    min_val = data[0]
    max_val = data[-1]
    result = normalize_array(data, NormalizationType.MIN_MAX)
    diff = max_val - min_val
    for i, x in enumerate(result):
        assert x == (data[i] - 1.1) / diff
