#!/usr/bin/env python3

# TensorFlow implementation of a shallow neural network for the kaggle shelter animals competition

import tensorflow as tf
import random

class ShallowNet(object):
    def __init__(self, nInputs = 6, nOutcomes = 5, nHidden = 1024):
        self.nInputs = nInputs
        self.nOutcomes = nOutcomes
        self.nHidden = nHidden
        random.seed()
        self.setupNetwork()

    def setupNetwork(self):
        """ setup the neural net. After this has been called, the object will
        have the following properties:
        sess: the TF session
        predictions: the final predictions
        trainStep: the function to execute on each training step
        accuracy: the current accuracy
        """
        # setup the session
        self.sess = tf.Session()

        # inputs and outputs
        self.x = tf.placeholder(tf.float32, shape=[None, self.nInputs])
        self.y = tf.placeholder(tf.float32, shape=[None, self.nOutcomes])

        # first hidden layer, fully connected
        w_fc1 = ShallowNet.weightVariable([self.nInputs, self.nHidden])
        b_fc1 = ShallowNet.biasVariable([self.nHidden])
        neurons1 = tf.nn.relu(tf.matmul(self.x, w_fc1) + b_fc1)
        # drop out for regularization
        self.keepProb = tf.placeholder(tf.float32)
        dropoutNeurons1 = tf.nn.dropout(neurons1, self.keepProb)

        # output layer, fully connected
        w_fc2 = ShallowNet.weightVariable([self.nHidden, self.nOutcomes])
        b_fc2 = ShallowNet.biasVariable([self.nOutcomes])
        self.predictions = tf.nn.softmax(tf.matmul(dropoutNeurons1, w_fc2) + b_fc2)

        # use cross entropy as the penalty function
        crossEntropy = tf.reduce_mean(-tf.reduce_sum(self.y * tf.log(self.predictions), reduction_indices=[1]))

        # use Adam optimizer for the training step
        self.trainStep = tf.train.AdamOptimizer(1e-4).minimize(crossEntropy)
        correctPrediction = tf.equal(tf.argmax(self.predictions, 1), tf.argmax(self.y, 1))
        self.accuracy = tf.reduce_mean(tf.cast(correctPrediction, tf.float32))

        # initialize all variables
        self.sess.run(tf.initialize_all_variables())

    def runTrainStep(self, xdata, ydata):
        """ run a training step using xdata and ydata, with 50% neurons dropout """
        try:
            self.sess.run(self.trainStep, feed_dict = {self.x: xdata, self.y: ydata, self.keepProb: 0.5})
        except AttributeError:
            print("Error: you have to call setupNetwork() before training.")

    def currentAccuracy(self, xdata, ydata):
        """ return the current accuracy on xdata and ydata """
        try:
            return self.sess.run(self.accuracy, feed_dict = {self.x: xdata, self.y: ydata, self.keepProb: 1.0})
        except AttributeError:
            print("Error: you have to call setupNetwork() before calculating the accuracy.")
            return 0.0

    def trainOnRandomBatch(self, xdata, ydata, N=50, returnAccuracy=False):
        """ take a random batch of N data points out of xdata and ydata and train on this """
        # make copies of the original arrays:
        jointData = list(zip(xdata, ydata))
        pickedData = random.sample(jointData, N)
        newX = [x[0] for x in pickedData]
        newY = [y[1] for y in pickedData]
        self.runTrainStep(newX, newY)
        if returnAccuracy:
            return self.currentAccuracy(newX, newY)
        else:
            return 0.0


    @staticmethod
    def weightVariable(shape):
        initial = tf.truncated_normal(shape, stddev=0.1)
        return tf.Variable(initial)

    @staticmethod
    def biasVariable(shape):
        initial = tf.constant(0.1, shape=shape)
        return tf.Variable(initial)