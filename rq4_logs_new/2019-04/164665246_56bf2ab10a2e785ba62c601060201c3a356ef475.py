import tensorflow as tf
import numpy as np
import matplotlib.pyplot as plt

from tensorflow.examples.tutorials.mnist import input_data

mnist = input_data.read_data_sets("/tmp/data/", one_hot=True)

learning_rate = 0.02
training_epochs = 100
batch_size = 256
display_step = 1
examples_to_show = 10
input_size = 784
hidden_size1 = 256
hidden_size2 = 128

x = tf.placeholder(tf.float32, shape=[None, input_size])

def build_AE(x):
    W1 = tf.Variable(tf.random_normal(shape=[input_size, hidden_size1]))
    b1 = tf.Variable(tf.random_normal(shape=[hidden_size1]))
    output_h1 = tf.nn.sigmoid(tf.matmul(x, W1) + b1)
    W2 = tf.Variable(tf.random_normal(shape=[hidden_size1, hidden_size2]))
    b2 = tf.Variable(tf.random_normal(shape=[hidden_size2]))
    output_h2 = tf.nn.sigmoid(tf.matmul(output_h1, W2) + b2)
    W3 = tf.Variable(tf.random_normal(shape=[hidden_size2, hidden_size1]))
    b3 = tf.Variable(tf.random_normal(shape=[hidden_size1]))
    output_h3 = tf.nn.sigmoid(tf.matmul(output_h2, W3) + b3)
    W4 = tf.Variable(tf.random_normal(shape=[hidden_size1, input_size]))
    b4 = tf.Variable(tf.random_normal(shape=[input_size]))
    x_re = tf.nn.sigmoid(tf.matmul(output_h3, W4) + b4)

    return x_re

y_pred = build_AE(x)

y_result = x

loss = tf.reduce_mean(tf.pow(y_result - y_pred, 2))
train_step = tf.train.RMSPropOptimizer(learning_rate).minimize(loss)

with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())

    for epoch in range(training_epochs):
        total_batch = int(mnist.train.num_examples/batch_size)

        for i in range(total_batch):
            batch_xs, batch_ys = mnist.train.next_batch(batch_size)
            _, current_loss = sess.run([train_step, loss], feed_dict={x : batch_xs})

        if epoch % display_step == 0:
            print("epoch : %d, loss : %f" % ((epoch+1), current_loss))

    result = sess.run(y_pred, feed_dict={x : mnist.test.images[:examples_to_show]})
    f, a = plt.subplots(2, 10, figsize = (10, 2))
    for i in range(examples_to_show):
        a[0][i].imshow(np.reshape(mnist.test.images[i], (28, 28)))
        a[1][i].imshow(np.reshape(result[i], (28, 28)))

    f.savefig('result_AE_image.png')

    f.show()
    plt.draw()
    plt.waitforbuttonpress()