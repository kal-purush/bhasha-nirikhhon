import numpy as np
import cmath
from math import sqrt

hs2 = 1 / sqrt(2)
offset = 0.000001

pauliX = np.matrix([[0,1],[1,0]])
pauliY = np.matrix([[0,1j],[1j,0]])
pauliZ = np.matrix([[1,0],[0,-1]])
hadamard = hs2 * np.matrix([[1, 1], [1, -1]])


class Qubit:
    def __init__(self, a, b):
        if 1 - offset < abs(a * a) + abs(b * b) < 1 + offset:
            self.a = a
            self.b = b
            self.matrix = np.matrix([[self.a],[self.b]])
        else:
           raise impossibleQubitException("Expected A^2 + B^2 = 1, but was " + str(abs(a * a) + abs(b * b)), None) 
       
    def pauliX(self):
        tmp = pauliX * self.matrix
        return Qubit(tmp.item(0), tmp.item(1))

    def pauliY(self):
        tmp = pauliY * self.matrix
        return Qubit(tmp.item(0), tmp.item(1))

    def pauliZ(self):
        tmp = pauliZ * self.matrix
        return Qubit(tmp.item(0), tmp.item(1))

    def hadamard(self):
        tmp = hadamard * self.matrix
        return Qubit(tmp.item(0), tmp.item(1))

    def __str__(self):
        return str(self.a) + "|0> + " + str(self.b) + "|1>"

class impossibleQubitException(Exception):
    def __init__(self, message, errors):
        
        super().__init__(message)
        self.errors = errors