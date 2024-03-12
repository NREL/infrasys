import numpy as np

from infrasys.normalization import NormalizationType, normalize_array


def test_normalization_max():
    data = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
    result = normalize_array(data, NormalizationType.MAX)
    for i, x in enumerate(result):
        assert x == data[i] / 5.5
