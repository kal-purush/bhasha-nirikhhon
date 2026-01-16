from NeuralFactory import neural_network_factory
import random
import tensorflow as tf
import numpy as np

# Make program determenistic
random.seed(123)
np.random.seed(123)
tf.set_random_seed(123)

network, cases = neural_network_factory("configs/parity.json")
network.train_model(cases)