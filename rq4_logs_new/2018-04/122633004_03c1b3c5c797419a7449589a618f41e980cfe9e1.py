#!/usr/bin/env python3
# vim: set fileencoding=utf-8 :

"""Implements Recurrent Highway Networks from Zilly et al. (2017)."""

import torch
import torch.nn as nn
from torch.autograd import Variable

from pytorch_lm.dropout import StatefulDropout
from pytorch_lm.utils.config import create_object


class Rhn(nn.Module):
    """Implements Recurrent Highway Networks from Zilly et al. (2017)."""
    def __init__(self, input_size, hidden_size, num_layers, dropout=0):
        super(Rhn, self).__init__()
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        self.w_h = nn.Parameter(torch.Tensor(input_size, hidden_size))
        self.w_t = nn.Parameter(torch.Tensor(input_size, hidden_size))
        self.w_c = nn.Parameter(torch.Tensor(input_size, hidden_size))
        self.r_h = [nn.Linear(hidden_size, hidden_size)
                    for l in range(self.num_layers)]
        self.r_t = [nn.Linear(hidden_size, hidden_size)
                    for l in range(self.num_layers)]
        self.r_c = [nn.Linear(hidden_size, hidden_size)
                    for l in range(self.num_layers)]
        for letter, lst in [('H', self.r_h), ('T', self.r_t), ('C', self.r_c)]:
            for l, p in enumerate(lst, 1):
                self.add_module('Rb_{}_{}'.format(letter, l), p)

        self.reset_parameters()

    def forward(self, input, s):
        outputs = []

        print('INPUT', input, 'S-1', s)
        # chunk() cuts batch_size x 1 x input_size chunks from input
        for input_t in map(torch.squeeze, input.chunk(input.size(1), dim=1)):
            print('INPUT_T', input_t)
            for l in range(self.num_layers):
                print('L', l)
                # The input is processed only by the first layer
                whx = input_t.matmul(self.w_h) if l == 0 else 0
                wtx = input_t.matmul(self.w_t) if l == 0 else 0
                wcx = input_t.matmul(self.w_c) if l == 0 else 0
                print('WHX', whx)
                print('WTX', wtx)
                print('WCX', wcx)

                # The gates (and the state)
                h = torch.tanh(whx + self.r_h[l](s))
                t = torch.sigmoid(wtx + self.r_t[l](s))
                c = torch.sigmoid(wcx + self.r_c[l](s))
                print('H', h)
                print('T', t)
                print('C', c)

                # The new state
                s = h * t + s * c
                print('S', s)

            # Here the output is the current s
            print('OUTPUT')
            outputs.append(s)
        return torch.stack(outputs, 1), s

    def reset_parameters(self, initrange=0.1):
        """Initializes the parameters uniformly to between -/+ initrange."""
        for weight in self.parameters():
            weight.data.uniform_(-initrange, initrange)

    def init_hidden(self, batch_size):
        """
        Returns a :class:`Variable` for the hidden state. As I understand, we
        only need one of these (as opposed to LSTM).
        """
        return Variable(torch.Tensor(
            batch_size, self.hidden_size).zero_().type(self.w_h.type()))