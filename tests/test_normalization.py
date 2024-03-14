import numpy as np

from infrasys.normalization import NormalizationMax, NormalizationByValue


def test_normalization_max():
    data = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
    normalization = NormalizationMax()
    result = normalization.normalize_array(data)
    assert normalization.max_value == data[-1]
    for i, x in enumerate(result):
        assert x == data[i] / 5.5


def test_normalization_by_value():
    data = np.array([1.1, 2.2, 3.3, 4.4, 5.5])
    normalization = NormalizationByValue(value=data[-1])
    result = normalization.normalize_array(data)
    for i, x in enumerate(result):
        assert x == data[i] / 5.5
