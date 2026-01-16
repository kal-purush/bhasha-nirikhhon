import numpy as np
import theano.tensor as T
import theano

rand_gen = numpy.random

X = T.matrix('x')
Y = T.vector('y')

k = 3

num_feat = len(X.T)

distances = np.array([])






