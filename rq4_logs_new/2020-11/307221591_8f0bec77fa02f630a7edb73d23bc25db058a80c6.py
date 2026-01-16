import sys

import numpy as np
import pytest

sys.path.append('./cnn')
sys.path.append("./shared")
import convolution_layer as cl

def test_concolution_layer_forwordpropagation():
    a = np.zeros([1, 4, 4])
    count = 0
    for i in range(4):
        for j in range(4):
            a[0][i][j] = count
            count += 1
    conv = cl.Convolution_Layer(1, 2, "test")
    out = conv.forwordpropagation(a)
    ans = np.array([[3.4, 4.4, 5.4], [7.4, 8.4, 9.4], [11.4, 12.4, 13.4]])
    assert ((out - ans) < 1e-3).all()