__author__ = 'liushuman'

from Preprocessor import Preprocessor

from collections import OrderedDict
from numpy import *

import numpy
import pickle
import theano
import theano.tensor as T

import sys
sys.path.append("../..")
from models.DNNUnit import DNNUnit
from models.Pooling import Pooling
from optimal.AdaDelta import AdaDelta


class Reasoner(object):

    def __init__(self):
        self.preprocessor = Preprocessor()
        self.cin = 40
        self.cinter = 20
        self.co = 10
        self.reason_layer = 2
        self.dnn_size = [(32, 40), (1, 32), (20, 32), (1, 20)]

        self.params = []

    def apply(self, _question, _factors, _y):

        for _r in range(self.reason_layer):
            # input: vector, matrix, layer
            # output: vector
            _question = self.interact(_question, _factors)

        dnn_output = DNNUnit((self.co, self.cinter), (1, self.co))
        self.params += dnn_output.params

        _output = T.dot(dnn_output.weight, _question) + dnn_output.bias     # vector 1*10
        _output = T.nnet.softmax(_output)[0]                                # vector 1*10

        _y_pred = T.argmax(_output)
        _nll = -T.log(_output)[_y]
        _grads = T.grad(_nll, self.params)
        return _y_pred, _nll, _grads, _factors, _y

    def interact(self, _q_, _f_):

        dnn_list = []
        for i in range(0, len(self.dnn_size)/2):
            dnn = DNNUnit(self.dnn_size[2*i], self.dnn_size[2*i+1])
            dnn_list.append(dnn)
            self.params += dnn.params

        def _interact(__f, __q):
            inter_layer = T.concatenate([__q, __f])
            for dnn in dnn_list:
                inter_layer = dnn.apply(inter_layer)[0]
            return inter_layer        # vector

        inter_layers, _ = theano.scan(_interact, sequences=_f_, non_sequences=[_q_])       # matrix 5*20
        inter_layer = Pooling().apply(inter_layers, 'mean')     # vector 1*20

        return inter_layer


if __name__ == "__main__":
    preprocessor = Preprocessor()
    reasoner = Reasoner()

    question = T.dvector('question')
    factors = T.dmatrix('factors')
    y = T.iscalar('y')
    lr = T.scalar('lr')
    # q is vector, factor is matrix 5*20

    _y_pred, _nll, _grads, _factors, _y = reasoner.apply(question, factors, y)

    updates = OrderedDict((p, p-lr*g) for p, g in zip(reasoner.params, _grads))

    fn = theano.function([question, factors, y, lr], [_y_pred, _nll], updates=updates)
    fn_test = theano.function([question, factors], _y_pred)

    iteration_num = 0
    for _ in range(500):
        iteration_num += 1
        print "iteration: ", iteration_num

        total_nll = 0
        precision = 0
        for pair in preprocessor.train_pairs:
            question_encode, factors_encode, label = preprocessor.generate_training_data(pair)
            [y_pred, nll] = fn(question_encode, factors_encode, label, 0.08)
            total_nll += nll
            if y_pred == label:
                precision += 1
        print "training nll", total_nll
        print "training precision", str(precision) + "/10000"

        precision = 0
        for pair in preprocessor.test_pairs:
            question_encode, factors_encode, label = preprocessor.generate_training_data(pair)
            y_pred = fn_test(question_encode, factors_encode)
            if y_pred == label:
                precision += 1
        print "testing precision", str(precision) + "/1000"