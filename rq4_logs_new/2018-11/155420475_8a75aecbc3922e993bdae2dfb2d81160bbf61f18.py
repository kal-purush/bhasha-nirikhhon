from __future__ import print_function

import argparse
import gzip
import json
import os
import pickle

import numpy as np
import tensorflow as tf
import matplotlib
import matplotlib.pyplot as plt
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
plt.switch_backend('agg')

def one_hot(labels):
    """this creates a one hot encoding from a flat vector:
    i.e. given y = [0,2,1]
     it creates y_one_hot = [[1,0,0], [0,0,1], [0,1,0]]
    """
    classes = np.unique(labels)
    n_classes = classes.size
    one_hot_labels = np.zeros(labels.shape + (n_classes,))
    for c in classes:
        one_hot_labels[labels == c, c] = 1
    return one_hot_labels


def mnist(datasets_dir='./data'):
    if not os.path.exists(datasets_dir):
        os.mkdir(datasets_dir)
    data_file = os.path.join(datasets_dir, 'mnist.pkl.gz')
    if not os.path.exists(data_file):
        print('... downloading MNIST from the web')
        try:
            import urllib
            urllib.urlretrieve('http://google.com')
        except AttributeError:
            import urllib.request as urllib
        url = 'http://www.iro.umontreal.ca/~lisa/deep/data/mnist/mnist.pkl.gz'
        urllib.urlretrieve(url, data_file)

    print('... loading data')
    # Load the dataset
    f = gzip.open(data_file, 'rb')
    try:
        train_set, valid_set, test_set = pickle.load(f, encoding="latin1")
    except TypeError:
        train_set, valid_set, test_set = pickle.load(f)
    f.close()

    test_x, test_y = test_set
    test_x = test_x.astype('float32')
    test_x = test_x.astype('float32').reshape(test_x.shape[0], 28, 28, 1)
    test_y = test_y.astype('int32')
    valid_x, valid_y = valid_set
    valid_x = valid_x.astype('float32')
    valid_x = valid_x.astype('float32').reshape(valid_x.shape[0], 28, 28, 1)
    valid_y = valid_y.astype('int32')
    train_x, train_y = train_set
    train_x = train_x.astype('float32').reshape(train_x.shape[0], 28, 28, 1)
    train_y = train_y.astype('int32')
    print('... done loading data')
    return train_x, one_hot(train_y), valid_x, one_hot(valid_y), test_x, one_hot(test_y)

def neural_network_model(data, nf=16 ,fs=3):

    """Model function for CNN."""
    
    # Input Layer
    input_layer = tf.reshape(data, [-1, 28, 28, 1])

    # Convolutional Layer #1
    conv1 = tf.layers.conv2d(
      inputs=input_layer,
      filters=nf,
      kernel_size=[fs, fs],
      padding="same",
      activation=tf.nn.relu)

    # Pooling Layer #1
    pool1 = tf.layers.max_pooling2d(inputs=conv1, pool_size=[2, 2], strides=2)

    # Convolutional Layer #2 and Pooling Layer #2
    conv2 = tf.layers.conv2d(
      inputs=pool1,
      filters=nf,
      kernel_size=[fs, fs],
      padding="same",
      activation=tf.nn.relu)
    pool2 = tf.layers.max_pooling2d(inputs=conv2, pool_size=[2, 2], strides=2)

    # Dense Layer
    pool2_flat = tf.reshape(pool2, [-1, 7 * 7 * nf])
    dense = tf.layers.dense(inputs=pool2_flat, units=128, activation=tf.nn.relu)
    
    # Logits Layer
    output = tf.layers.dense(inputs=dense, units=10)
    return output

def train_validate_and_test(x_train, y_train, x_valid, y_valid, x_test, y_test,num_epochs, lr, num_filters, batch_size, filter_size):
    x = tf.placeholder('float', [None, 784])
    y = tf.placeholder('float')
    
    prediction = neural_network_model(x, nf= num_filters, fs= filter_size)
    cost = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits = prediction, labels = y))
    optimizer =  tf.train.GradientDescentOptimizer(lr).minimize(cost)
    correct = tf.equal(tf.argmax(prediction, 1), tf.argmax(y, 1))
    accuracy = tf.reduce_mean(tf.cast(correct, 'float'))

    learning_curve = [0] * num_epochs
    train_loss_history= np.zeros(num_epochs)

    with tf.Session() as sess:
        sess.run(tf.global_variables_initializer())

        for epoch in range(num_epochs):
            for _ in range(int(len(x_train)/ batch_size)):
                rand_index = np.random.choice(len(x_train), batch_size)
                batch_x = x_train[rand_index].reshape([-1,784])
                batch_y = y_train[rand_index]
                sess.run(optimizer, feed_dict = {x: batch_x , y: batch_y})

            # Calculate training accuracy and loss per epoch
            acc, loss = sess.run([accuracy, cost], feed_dict={x: batch_x, y: batch_y})
            print("Epoch " + str(epoch + 1) + " / " + str(num_epochs) + ": Epoch Training Loss= " + \
                "{:.6f}".format(loss) + ", Training Accuracy= " + \
                "{:.5f}".format(acc))

            # Calculate validation accuracy and loss per epoch
            val_acc = 0
            val_loss = 0
            for val_step in range(int(len(x_valid)/ batch_size)):
                val_batch_x = x_valid[val_step*batch_size: ((val_step+1)*batch_size)].reshape([-1,784])
                val_batch_y = y_valid[val_step*batch_size: ((val_step+1)*batch_size)]
                step_acc, step_loss = sess.run([accuracy, cost], feed_dict={x: val_batch_x, y: val_batch_y})
                val_acc += step_acc
                val_loss += step_loss
            
            val_acc = val_acc / (val_step+1)
            val_loss = val_loss / (val_step+1)
            print("Validation set accuracy: %s" % val_acc)

            learning_curve[epoch] = val_loss
            train_loss_history[epoch] = loss
        print("Training Finished!")
        
        test_acc = sess.run(accuracy, feed_dict={x: x_test.reshape([-1,784]), y: y_test})
        print("Testing Accuracy:", test_acc)

        test_error = 1.0 - test_acc
        print('Val Curve', learning_curve)
    return learning_curve, test_error

def different_learning_rates(x_train, y_train, x_valid, y_valid,x_test, y_test, epochs, num_filters, batch_size, filter_size):
    #Implementing Part2 of the exercise
    x_axis = np.arange(epochs)
    for lr in [0.1, 0.01, 0.001, 0.0001]:
        learning_curve, test_error = train_validate_and_test(x_train, y_train, x_valid, y_valid,x_test, y_test, epochs, lr, num_filters, batch_size, filter_size)
        plt.plot(x_axis, learning_curve)
    plt.legend(['0.1', '0.01', '0.001', '0.0001'], loc='upper right')
    plt.savefig('different_learning_rates.png')

def different_filter_sizes(x_train, y_train, x_valid, y_valid,x_test, y_test, epochs, lr, num_filters, batch_size):
    #Implementing Part3 of the exercise
    x_axis = np.arange(epochs)
    for filter_size in [1,3,5,7]:
        learning_curve, test_error = train_validate_and_test(x_train, y_train, x_valid, y_valid,x_test, y_test, epochs, lr, num_filters, batch_size, filter_size)
        plt.plot(x_axis, learning_curve)
    plt.legend(['1x1', '3x3', '5x5', '7x7'], loc='upper right')
    plt.savefig('different_filter_sizes.png')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_path", default="./", type=str, nargs="?",
                        help="Path where the results will be stored")
    parser.add_argument("--input_path", default="./", type=str, nargs="?",
                        help="Path where the data is located. If the data is not available it will be downloaded first")
    parser.add_argument("--learning_rate", default=0.0739, type=float, nargs="?", help="Learning rate for SGD")
    parser.add_argument("--num_filters", default=49, type=int, nargs="?",
                        help="The number of filters for each convolution layer")
    parser.add_argument("--batch_size", default=31, type=int, nargs="?", help="Batch size for SGD")
    parser.add_argument("--epochs", default=50, type=int, nargs="?",
                        help="Determines how many epochs the network will be trained")
    parser.add_argument("--run_id", default=0, type=int, nargs="?",
                        help="Helps to identify different runs of an experiments")
    parser.add_argument("--filter_size", default=5, type=int, nargs="?",
                        help="Filter width and height")
    args = parser.parse_args()

    # hyperparameters
    lr = args.learning_rate
    num_filters = args.num_filters
    batch_size = args.batch_size
    epochs = args.epochs
    filter_size = args.filter_size

    # train and test convolutional neural network
    x_train, y_train, x_valid, y_valid, x_test, y_test = mnist(args.input_path)

    learning_curve, test_error = train_validate_and_test(x_train, y_train, x_valid, y_valid,x_test, y_test, epochs, lr, num_filters, batch_size, filter_size)

    different_learning_rates(x_train, y_train, x_valid, y_valid,x_test, y_test, epochs, num_filters, batch_size, filter_size)
    different_filter_sizes(x_train, y_train, x_valid, y_valid,x_test, y_test, epochs, lr, num_filters, batch_size)

    # save results in a dictionary and write them into a .json file
    results = dict()
    results["lr"] = lr
    results["num_filters"] = num_filters
    results["batch_size"] = batch_size
    results["filter_size"] = filter_size
    results["learning_curve"] = learning_curve
    results["test_error"] = test_error

    path = os.path.join(args.output_path, "results")
    os.makedirs(path, exist_ok=True)

    fname = os.path.join(path, "results_run_%d.json" % args.run_id)

    fh = open(fname, "w")
    json.dump(results, fh)
    fh.close()