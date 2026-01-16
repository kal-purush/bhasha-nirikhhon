#!/usr/local/bin python

import numpy as np # linear algebra
import tensorflow as tf
import read_data as rd
import sklearn
from sklearn.model_selection import train_test_split
import cv2
import matplotlib.pyplot as plt
from  Datakeeper import *
from tensorflow.contrib.boosted_trees.lib.learner import batch
from termcolor import colored

#tf.set_random_seed(2)
#np.random.seed(2)

# Training Parameters
learning_rate = 0.001
num_steps = 2000
batch_size = 32

# Network Parameters
num_input = IMG_HEIGHT*IMG_WIDTH*3
num_classes = 6
dropout = 0.25 # Dropout, probability to drop a unit

# Create the neural network
def conv_net(x_dict, n_classes, dropout, reuse, is_training):
    # Define a scope for reusing the variables
    with tf.variable_scope('ConvNet', reuse=reuse):
        # TF Estimator input is a dict, in case of multiple inputs
        x = x_dict['images']

        # Reshape to match picture format [Height x Width x Channel]
        # Tensor input become 4-D: [Batch Size, Height, Width, Channel]
        x = tf.reshape(x, shape=[-1, IMG_HEIGHT, IMG_WIDTH, 3])

        # Convolution Layer with 128 filters and a kernel size of 5
        conv1 = tf.layers.conv2d(x, 128, 5, activation=tf.nn.relu)
        # Max Pooling (down-sampling) with strides of 2 and kernel size of 2
        conv1 = tf.layers.max_pooling2d(conv1, 2, 2)

        # Convolution Layer with 256 filters and a kernel size of 3
        conv2 = tf.layers.conv2d(conv1, 256, 3, activation=tf.nn.relu)
        # Max Pooling (down-sampling) with strides of 2 and kernel size of 2
        conv2 = tf.layers.max_pooling2d(conv2, 2, 2)

        # Flatten the data to a 1-D vector for the fully connected layer
        fc1 = tf.contrib.layers.flatten(conv2)

        # Fully connected layer (in tf contrib folder for now)
        fc1 = tf.layers.dense(fc1, 1024)
        # Apply Dropout (if is_training is False, dropout is not applied)
        fc1 = tf.layers.dropout(fc1, rate=dropout, training=is_training)

        # Output layer, class prediction
        out = tf.layers.dense(fc1, n_classes)

    return out

# Define the model function (following TF Estimator Template)
def model_fn(features, labels, mode):
    # Build the neural network
    # Because Dropout have different behavior at training and prediction time, we
    # need to create 2 distinct computation graphs that still share the same weights.
    logits_train = conv_net(features, num_classes, dropout, reuse=False,
                            is_training=True)
    logits_test = conv_net(features, num_classes, dropout, reuse=True,
                           is_training=False)

    # Predictions
    pred_classes = tf.argmax(logits_test, axis=1)
    pred_probas = tf.nn.softmax(logits_test)

    # If prediction mode, early return
    if mode == tf.estimator.ModeKeys.PREDICT:
        return tf.estimator.EstimatorSpec(mode, predictions=pred_classes)

        # Define loss and optimizer
    loss_op = tf.reduce_mean(tf.nn.sparse_softmax_cross_entropy_with_logits(
        logits=logits_train, labels=tf.cast(labels, dtype=tf.int32)))
    optimizer = tf.train.AdamOptimizer(learning_rate=learning_rate)
    train_op = optimizer.minimize(loss_op,
                                  global_step=tf.train.get_global_step())

    # Evaluate the accuracy of the model
    acc_op = tf.metrics.accuracy(labels=labels, predictions=pred_classes)

    # TF Estimators requires to return a EstimatorSpec, that specify
    # the different ops for training, evaluating, ...
    estim_specs = tf.estimator.EstimatorSpec(
        mode=mode,
        predictions=pred_classes,
        loss=loss_op,
        train_op=train_op,
        eval_metric_ops={'accuracy': acc_op})

    return estim_specs


def get_data():
    image_list_ , label_list_ = rd.read_labeld_image_list()

    image_list = []
    label_list = []

    for path , lable in zip(image_list_ , label_list_) :
        if "english" in lable or "spanish" in lable or "arabic" in lable or "mandarin" in lable or "french" in lable or "russian" in lable:
            image_list.append(path)
            label_list.append(lable)
    label_names = list(set(label_list))

    numeric_labels = []
    for l in label_list :
        numeric_labels.append(label_names.index(l))

    data_image_train, data_image_test , data_label_train , data_label_test = train_test_split(image_list , numeric_labels , test_size=0.15)

    train_data  = DataKeeper(data_image_train,data_label_train, label_names )
#  train_data.setBatchSize(batch_size)
    test_data   = DataKeeper(data_image_test,data_label_test, label_names )
    data = {
        'test': test_data,
        'train': train_data
    }

    return data


def main():
    # Parameters
    learning_rate = 0.0001
    training_epochs =100
    batch_size = 96
    display_step = 1

    data = get_data()
    # Build the Estimator
    model = tf.estimator.Estimator(model_fn)

    # Define the input function for training
    input_fn = tf.estimator.inputs.numpy_input_fn(
        x={'images': data['train'].getNextBatch()[0]}, y=data['train'].getNextBatch()[1],
        batch_size=batch_size, num_epochs=None, shuffle=True)
    # Train the Model
    model.train(input_fn, steps=num_steps)

    # Evaluate the Model
    # Define the input function for evaluating
    input_fn = tf.estimator.inputs.numpy_input_fn(
        x={'images': data['test'].getNextBatch()[0]}, y=data['test'].getNextBatch()[1],
        batch_size=batch_size, shuffle=False)
    # Use the Estimator 'evaluate' method
    e = model.evaluate(input_fn)

    print("Testing Accuracy:", e['accuracy'])
    return


if __name__ == "__main__":
    main()