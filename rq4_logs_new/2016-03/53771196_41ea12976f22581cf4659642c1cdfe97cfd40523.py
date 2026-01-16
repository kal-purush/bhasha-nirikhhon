#!/usr/bin/python
"""
File: midterm.py

Copyright (c) 2016 Michael Seaman

License: MIT

Calculates sequence values of the recursive definition
given in eq. 1

"""
import numpy as np


def sequence(x0, r, N = 100):
    """
    Outputs a pair of numpy arrays, one being the sequence
    number, and the second being the value at that position
    in the sequence given by eq. 1:
    x[n+1] = r * x[n] * (1 - x[n])
    """
    xList = np.arange(1, N+1)
    yList = np.zeros(N)
    yList[0] = x0
    for i in xrange(1, N):
        yList[i] = r * yList[i-1] * (1 - yList[i-1])    
    return xList, yList

def test_sequence_on_r0():
    """
    Tests sequence function on the value r = 0, where
    all values besides the initial should be zero
    """
    sequence_output = sequence(.5, 0)[1]
    assert np.all(sequence_output[1:] == np.zeros(99))
    assert (sequence_output[0] == .5)

def test_sequence_on_x0():
    """
    Tests sequence function on the value x0 = 0, where
    all values should be zero
    """
    sequence_output = sequence(0, 2.5)[1]
    assert np.all(sequence_output == np.zeros(100))

def test_sequence_on_2():
    """
    Tests the sequence function on the values x0= .5
    and r = 2, where the output should be .5 everytime.
    """
    sequence_output = sequence(.5, 2)[1]
    a = np.empty(100)
    a.fill(.5)
    assert np.all(sequence_output == a)
