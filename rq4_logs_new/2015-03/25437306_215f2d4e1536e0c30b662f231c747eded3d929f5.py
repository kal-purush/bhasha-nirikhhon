"""
"""
from __future__ import division, print_function, absolute_import

import numpy as np


__all__ = ['HighLevelInterface', 'VanillaAdjointOperator', 'PowerOperator',
           'ConcreteInterface', 'ExtendedAdjointOperator',
           'ExtendedMatrixOperator']


class HighLevelInterface(object):
    # This interface expects to be mixed into a class with the following members
    # _matmat (function)
    # _transpose (function) # _adjoint (function)
    @property
    def T(self):
        return self._transpose()
    @property
    def H(self):
        return self._adjoint()
    def dot(self, other):
        return self._matmat(other)


class VanillaAdjointOperator(HighLevelInterface):
    # The forward operator is expected to meet a few extra conditions.
    # It should be real-valued (so the adjoint is the transpose).
    # It should also be square but not necessarily symmetric.
    # This class is not mixed with _ConcreteInterface.
    def __init__(self, L):
        self._L = L
        self.args = (L, )
    @property
    def shape(self):
        return self._L.shape
    @property
    def dtype(self):
        return self._L.dtype
    def _matmat(self, other):
        return self._L._my_adjoint_matmat(other)
    def _adjoint(self):
        return self._L
    def _transpose(self):
        return self._L


class ExtendedAdjointOperator(HighLevelInterface):
    # The forward operator is expected to meet a few extra conditions.
    # It should be real-valued (so the adjoint is the transpose),
    # and it should have easily computable row and column vector 1-norms.
    # It should also be square but not necessarily symmetric.
    # This class is not mixed with _ConcreteInterface.
    def __init__(self, L):
        self._L = L
        self.args = (L, )
    @property
    def shape(self):
        return self._L.shape
    @property
    def dtype(self):
        return self._L.dtype
    def _matmat(self, other):
        return self._L._my_adjoint_matmat(other)
    def abs_sum_axis_0(self):
        return self._L.abs_sum_axis_1()
    def abs_sum_axis_1(self):
        return self._L.abs_sum_axis_0()
    def one_norm(self):
        return self._L.inf_norm()
    def inf_norm(self):
        return self._L.one_norm()
    def _adjoint(self):
        return self._L
    def _transpose(self):
        return self._L


class PowerOperator(HighLevelInterface):
    def __init__(self, L, p):
        self._L = L
        self._p = p
        self.__adj = None
    @property
    def shape(self):
        return self._L.shape
    @property
    def dtype(self):
        return self._L.dtype
    def _matmat(self, other):
        for i in range(self._p):
            other = self._L.dot(other)
        return other
    def _my_adjoint_matmat(self, other):
        for i in range(self._p):
            other = self._L.H.dot(other)
        return other
    def _transpose(self):
        if self.__adj is None:
            self.__adj = _VanillaAdjointOperator(self)
        return self.__adj
    def _adjoint(self):
        return self._transpose()


class ConcreteInterface(object):
    # This interface expects to be mixed into a class with the following members
    # abs_sum_axis_0 (function)
    # abs_sum_axis_1 (function)
    # _matmat (function)
    # _my_adjoint_matmat (function)
    # shape (property)
    # dtype (property)

    def _init_concrete_cache(self):
        self._one_norm = None
        self._inf_norm = None
        self.__adj = None

    def one_norm(self):
        if self._one_norm is None:
            self._one_norm = np.max(self.abs_sum_axis_0())
        return self._one_norm

    def inf_norm(self):
        if self._inf_norm is None:
            self._inf_norm = np.max(self.abs_sum_axis_1())
        return self._inf_norm

    def _transpose(self):
        if self.__adj is None:
            self.__adj = ExtendedAdjointOperator(self)
        return self.__adj

    def _adjoint(self):
        return self._transpose()


class ExtendedMatrixOperator(HighLevelInterface, ConcreteInterface):
    # This wraps a sparse matrix and has an interface common to this module.
    # The matrix should be square and real-valued.
    def __init__(self, M):
        self.dtype = M.dtype
        self.shape = M.shape
        self._M = M
        self.args = (M, )
        self._MT = None
        self._M_abs = None
        self._abs_sum_axis_0 = None
        self._abs_sum_axis_1 = None
        self._init_concrete_cache()

    def abs_sum_axis_0(self):
        if self._abs_sum_axis_0 is None:
            if self._M_abs is None:
                self._M_abs = np.absolute(self._M)
            self._abs_sum_axis_0 = self._M_abs.sum(axis=0).A.ravel()
        return self._abs_sum_axis_0

    def abs_sum_axis_1(self):
        if self._abs_sum_axis_1 is None:
            if self._M_abs is None:
                self._M_abs = np.absolute(self._M)
            self._abs_sum_axis_1 = self._M_abs.sum(axis=1).A.ravel()
        return self._abs_sum_axis_1

    def _matmat(self, other):
        return self._M.dot(other)

    def _my_adjoint_matmat(self, other):
        if self._MT is None:
            self._MT = self._M.T
        return self._MT.dot(other)