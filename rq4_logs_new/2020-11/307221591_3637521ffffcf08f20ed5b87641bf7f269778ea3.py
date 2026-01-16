import sys

import numpy as np
import pytest

sys.path.append("./dataset")
import logic_circuit as lc

def test_load_or():
    train_data, label = lc.dset("or", 1)
    assert (np.array([[0., 0.], [0., 1.], [1., 0.], [1., 1.]]) == train_data).all()
    assert (np.array([[0.], [1.], [1.], [1.]]) == label).all()

def test_load_and():
    train_data, label = lc.dset("and", 1)
    assert (np.array([[0., 0.], [0., 1.], [1., 0.], [1., 1.]]) == train_data).all()
    assert (np.array([[0.], [0.], [0.], [1.]]) == label).all()

def test_load_xor():
    train_data, label = lc.dset("xor", 1)
    assert (np.array([[0., 0.], [0., 1.], [1., 0.], [1., 1.]]) == train_data).all()
    assert (np.array([[0.], [1.], [1.], [0.]]) == label).all()

def test_load_nand():
    train_data, label = lc.dset("nand", 1)
    assert (np.array([[0., 0.], [0., 1.], [1., 0.], [1., 1.]]) == train_data).all()
    assert (np.array([[1.], [1.], [1.], [0.]]) == label).all()

def test_load_nand():
    train_data, label = lc.dset("w_not", 1)
    assert (np.array([[0., 0.], [0., 1.], [1., 0.], [1., 1.]]) == train_data).all()
    assert (np.array([[1., 1], [1., 0], [0, 1.], [0., 0]]) == label).all()

def test_load_cnn_ex():
    train_data, label = lc.dset("cnn_ex", 1)
    assert (np.array([[[255, 0, 0, 255], [0, 0, 0, 0], [0, 0, 0, 0], [255, 0, 0, 255]],
                      [[255, 255, 255, 255], [255, 0, 0, 255], [255, 0, 0, 255], [255, 255, 255, 255]],
                      [[0, 0, 255, 0], [255, 255, 255, 255], [0, 0, 255, 0], [0, 0, 255, 0]],
                      [[0, 0, 0, 0], [0, 255, 255, 0], [0, 255, 255, 0], [0, 0, 0, 255]]]) == train_data).all()
    assert (np.array([[1, 0, 0, 0], [0, 1, 0, 0], [0, 0, 1, 0], [0, 0, 0, 1]]) == label).all()