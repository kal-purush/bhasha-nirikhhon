__author__ = 'liushuman'

import theano
import theano.tensor as T

import sys
sys.path.append("..")
from utils.ActivationFunctions import ActivationFunctions
from utils.Params import Params


class DNNUnit(object):

    def __init__(self, weight_size, bias_size=None):
        self.weight = Params().uniform(weight_size)

        if bias_size:
            self._if_bias = True
            self.bias = Params().uniform(bias_size)
            self.params = [self.weight, self.bias]
        else:
            self._if_bias = False
            self.params = [self.weight]

    def apply(self, layer_input, activate_type='sigmoid'):
        layer_output = T.dot(self.weight, layer_input)
        if self._if_bias:
            layer_output += self.bias

        layer_output = ActivationFunctions().apply(layer_output, activate_type)

        return layer_output