import numpy as np
import pycpp.functions._C as F


def func(size, dtype):
    np.random.seed(0)
    x = np.random.randn(size, size).astype(dtype)
    np_result = np.matmul(x, x.T)
    pycpp_result = F.matmul(x, x.T)

    result = np.allclose(np_result, pycpp_result, rtol=1e-04, atol=1e-08)
    return result   


def test_double():
    size = 2**5
    result = func(size, np.float64)

    assert result


def test_float():
    size = 2**5
    result = func(size, np.float32)
    
    assert result
