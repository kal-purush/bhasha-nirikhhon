import os
import sys
import timeit
import scipy.io
import theano
import numpy as np
import theano.tensor as T

from theano.tensor.nnet import conv2d
from theano.tensor.nnet import softmax
from theano.tensor import shared_randomstreams

def ReLU(z):
	return T.maximum(0.0, z)

class FullyConnectedLayer(object):

	def __init__(self, rng, input, n_in, n_out, W=None, b=None,
				activation_fn=ReLU, p_dropout=0.5):
		self.rng = rng
		self.input = input
		self.n_in = n_in
		self.n_out = n_out
		self.activation_fn = activation_fn
		self.p_dropout=p_dropout
	
		if W is None:
			self.W = theano.shared(np.asarray(rng.uniform(low=-(6./(n_in+n_out)), high=(6./(n_in+n_out)),
					size=(n_in, n_out)), dtype=theano.config.floatX), name='W', borrow=True)
	
		if b is None:
			self.b = theano.shared(np.zeros((n_out,), dtype=theano.config.floatX), name='b', borrow=True)
	
		self.output = self.activation_fn((1-self.p_dropout)*T.dot(self.input, self.W) + self.b)
		self.y_out = T.argmax(self.output, axis=1)
		self.input_dropout = dropout_layer(input, self.p_dropout)
		self.output_dropout = self.activation_fn(T.dot(self.input_dropout, self.W) + self.b)
	
		self.params = [self.W, self.b]

	def accuracy(self, y):
		return T.mean(T.eq(y, self.y_out))

def dropout_layer(layer, p_dropout):
    srng = shared_randomstreams.RandomStreams(
        np.random.RandomState(0).randint(999999))
    mask = srng.binomial(n=1, p=1-p_dropout, size=layer.shape)
    return layer*T.cast(mask, theano.config.floatX)