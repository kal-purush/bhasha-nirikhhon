__author__ = 'liushuman'

import theano
import theano.tensor as T

import sys
sys.path.append("..")
from utils.ActivationFunctions import ActivationFunctions
from utils.Params import Params


class GRUUnit(object):

    def __init__(self, wx_size, wh_size, h_size, bias_size=None):
        self.reset_gate = ResetGate(wx_size, wh_size, bias_size)
        self.update_gate = UpdateGate(wx_size, wh_size, bias_size)

        self.weight_xt_candidate = Params().uniform(wx_size)
        self.weight_htm1_candidate = Params().uniform(wh_size)
        self.hidden0 = Params().constant(h_size)
        self.params = self.reset_gate.params + self.update_gate.params + \
                    [self.weight_xt_candidate, self.weight_htm1_candidate, self.hidden0]
        if bias_size:
            self._if_bias = True
            self.bias = Params().constant(bias_size)
            self.params.append(self.bias)
        else:
            self._if_bias = False

    def apply(self, x_sequences, activate_type='tanh'):

        def _step_forward(x_t, h_tm1):
            # x_t is vector, h_tm1 is matrix
            reset_t = self.reset_gate.apply(x_t, h_tm1[0])     # matrix/vector(no bias)
            update_t = self.update_gate.apply(x_t, h_tm1[0])   # matrix/vector(no bias)

            candidate_ht = T.dot(self.weight_xt_candidate, x_t) + \
                           T.dot(self.weight_htm1_candidate, (reset_t * h_tm1[0])[0])
            if self._if_bias:
                candidate_ht += self.bias       # matrix

            candidate_ht = ActivationFunctions().apply(candidate_ht, activate_type)     # matrix

            ht = (1-update_t) * h_tm1[0] + update_t * candidate_ht[0]   # matrix

            return ht

        hidden_layers, _ = theano.scan(fn=_step_forward, sequences=x_sequences, outputs_info=[self.hidden0])

        return hidden_layers    # a matrix n_step * 1 * cols


class ResetGate(object):

    def __init__(self, wx_size, wh_size, bias_size=None):
        self.weight_xt = Params().uniform(wx_size)
        self.weight_htm1 = Params().uniform(wh_size)
        self.params = [self.weight_xt, self.weight_htm1]

        if bias_size:
            self._if_bias = True
            self.bias = Params().constant(bias_size)
            self.params.append(self.bias)
        else:
            self._if_bias = False

    def apply(self, xt, htm1, activate_type='sigmoid'):
        gate_output = T.dot(self.weight_xt, xt) + T.dot(self.weight_htm1, htm1)
        if self._if_bias:
            gate_output += self.bias

        gate_output = ActivationFunctions().apply(gate_output, activate_type)

        return gate_output


class UpdateGate(object):

    def __init__(self, wx_size, wh_size, bias_size=None):
        self.weight_xt = Params().uniform(wx_size)
        self.weight_htm1 = Params().uniform(wh_size)
        self.params = [self.weight_xt, self.weight_htm1]

        if bias_size:
            self._if_bias = True
            self.bias = Params().constant(bias_size)
            self.params.append(self.bias)
        else:
            self._if_bias = False

    def apply(self, xt, htm1, activate_type='sigmoid'):
        gate_output = T.dot(self.weight_xt, xt) + T.dot(self.weight_htm1, htm1)
        if self._if_bias:
            gate_output += self.bias

        gate_output = ActivationFunctions().apply(gate_output, activate_type)

        return gate_output