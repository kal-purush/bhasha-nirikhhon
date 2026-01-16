__author__ = 'liushuman'

from Preprocessor import Preprocessor

from collections import OrderedDict
from numpy import *

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
        self.dnn_layer = 1
        self.reason_layer = 3

        try:
            f = open('Reasoner.weight', 'r')
            self.iteration_num = pickle.load(f)
            self.params = pickle.load(f)
            self._if_init = False
            f.close()
        except:
            self.iteration_num = 0
            self.params = []
            self._if_init = True

    def close_event(self):
        f = open('Reasoner.weight', 'w')
        pickle.dump(self.iteration_num, f)
        pickle.dump(self.params, f)
        f.close()

    def apply(self, _question, _factors, _y):

        for _r in range(self.reason_layer):
            _question = self.interact(_question, _factors)[0]

        dnn_output = DNNUnit((self.co, self.cinter), (1, self.co))
        if self._if_init:
            self.params += dnn_output.params
        _output = dnn_output.apply(_question)[0]

        _output = T.nnet.softmax(_output)

        _y_pred = T.argmax(_output)
        _nll = -T.log(_output)[y]
        _grads = T.grad(_nll, self.params)

        return _y_pred, _nll, _grads

    def interact(self, question, factors):

        def _interact(f, q):
            input_layer = T.concatenate([q, f])
            dnn = DNNUnit((self.cinter, self.cin), (1, self.cinter))
            inter_layer = dnn.apply(input_layer)
            if self._if_init:
                self.params += dnn.params
            return inter_layer

        inter_layers, updates = theano.scan(_interact,
                                            sequences=factors,
                                            non_sequences=[question])
        inter_layer = Pooling().apply(inter_layers, 'mean')
        return inter_layer


if __name__ == "__main__":
    preprocessor = Preprocessor()
    reasoner = Reasoner()

    # pair = preprocessor.train_pairs[0]

    # question_encode, factors_encode, label = preprocessor.generate_training_data(pair)
    # print question_encode, factors_encode, label

    question = T.dvector('question')
    factors = T.dmatrix('factors')
    y = T.iscalar('y')
    y_pred, nll, grads = reasoner.apply(question, factors, y)

    ada_delta = AdaDelta(reasoner.params)
    updates = ada_delta.apply(reasoner.params, grads)
    # updates = OrderedDict((p, p-g) for p, g in zip(reasoner.params, grads))

    train_sample = theano.function(inputs=[question, factors, y], outputs=[y_pred, nll], updates=updates)
    classify_sample = theano.function(inputs=[question, factors], outputs=y_pred)
    # fn2 = theano.function(inputs=[question, factors, y], outputs=grads)
    # print fn2(question_encode, factors_encode, label)
    # print train_sample(question_encode, factors_encode, label)
    # print fn2(question_encode, factors_encode, label)

    # reasoner.close_event()

    for _ in range(300):
        reasoner.iteration_num += 1
        print "iteration:", reasoner.iteration_num
        count = 0
        for pair in preprocessor.train_pairs:
            question_encode, factors_encode, label = preprocessor.generate_training_data(pair)
            [y_pred, nll] = train_sample(question_encode, factors_encode, label)
            if y_pred == label:
                count += 1
        print "train err ", str(count) + '/10000'

        count = 0
        for pair in preprocessor.test_pairs:
            question_encode, factors_encode, label = preprocessor.generate_training_data(pair)
            y_pred = classify_sample(question_encode, factors_encode)
            if y_pred == label:
                count += 1
        print "test err ", str(count) + '/1000'