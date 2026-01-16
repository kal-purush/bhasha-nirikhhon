import tensorflow as tf
import random
from util import tf_help

import time
import numpy as np
import math
import matplotlib.pyplot as plt


class P:
    """Params"""
    min_x = -10
    max_x = 10


def plot(x, y):
    plt.plot(x, y, antialiased=True, label='tf.sigmoid')
    plt.title("activation functions")
    plt.legend()
    plt.grid(True)
    plt.savefig('sigmoid_functions.png')


def run(activation):
    class G:
        xs = [[x] for x in np.arange(P.min_x, P.max_x, 0.01)]
        x = tf.constant(xs, tf.float32)
        y = activation(x)
        y_grad = tf.gradients([y], [x])

    with tf.Session() as sess:
        x, y, y_grad = sess.run((G.x, G.y, G.y_grad))
    return (x, y, y_grad)


# https://www.tensorflow.org/api_guides/python/nn#Activation_Functions

activations = [
    (tf.nn.sigmoid, 'sigmoid'),
    (tf.nn.relu, 'relu'),
    (tf.nn.crelu, 'crelu'),
    (tf.nn.relu6, 'relu6'),
    (tf.nn.elu, 'elu'),
    (tf.nn.selu, 'selu'),
    (tf.nn.softplus, 'softplus'),
    (tf.nn.softsign, 'softsign'),
    (tf.nn.tanh, 'tanh'),
]


for (activation, name) in activations:
    plt.title(name)

    (x, y, y_grad) = run(activation)
    plt.plot(x, y, antialiased=True, label=name)

    plt.plot(x, y_grad[0], antialiased=True, label="gradient")

    plt.grid(True)
    plt.legend()
    plt.savefig('../docs/activations/%s.png' % name)

    plt.close()