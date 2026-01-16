# -*- coding: utf-8 -*-
# 完成
# テンソルを減らす 47行目

import tensorflow as tf
import numpy as np
import time
import matplotlib.pyplot as plt
# import math

intercept=-0.25
coefficient1=0.5
coefficient2=5
coefficient3=5
coefficient4=0.2

start = time.time()

# data set
data_num = 200
tenosor_num = 100

# graph_para
graph_range = (-2, 2)
x_plot_sub = 0.01
x_range = int((graph_range[1] - graph_range[0])/x_plot_sub)

x_sample = np.random.rand(data_num,1).astype("float32")
x_sample = x_sample*2.0 - 1.0
y_sample = np.sin(10*x_sample) + intercept + np.random.rand(data_num,1).astype("float32")/2

x_data = tf.placeholder(tf.float32,[1])
y_data = tf.placeholder(tf.float32,[1])

w_input = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
b_input = tf.Variable(tf.zeros([1]))

w = tf.constant(tenosor_num,shape=[tenosor_num,1],dtype=tf.float32)
b = tf.constant(2.0*(np.arange(tenosor_num).astype(float)-(tenosor_num/2.0)),shape=[tenosor_num,1],dtype=tf.float32)
# w = tf.Variable(tf.constant(tenosor_num,shape=[tenosor_num,1],dtype=tf.float32))
# b = tf.Variable(tf.constant(2.0*(np.arange(tenosor_num).astype(float)-(tenosor_num/2.0)),shape=[tenosor_num,1],dtype=tf.float32))

# w_input2 = tf.Variable(tf.random_uniform([1], -1.0, 1.0))
# b_input2 = tf.Variable(tf.zeros([1]))
# w2 = tf.Variable(tf.random_uniform([4,4], -1.0, 1.0))
# b2 = tf.Variable(tf.zeros([4,4]))

w_output = tf.Variable(tf.random_uniform([2,tenosor_num], -1.0, 1.0))
b_output = tf.Variable(tf.zeros([1]))
# b_output2 = tf.Variable(tf.zeros([1]))

# 1st layer
# h_linear1 = x_data
h_linear1 = tf.add(w_input*x_data, b_input)

# 2nd layer
h_linear2 = tf.sigmoid(tf.add(h_linear1*w, b))

h_linear3 = tf.reduce_sum(tf.matmul(w_output, h_linear2))

# output
y = tf.add(h_linear3,b_output)

# # Add summary ops to collect data
# w_hist = tf.histogram_summary("weights", w)
# b_hist = tf.histogram_summary("biases", b)
# y_hist = tf.histogram_summary("y", y)

loss = tf.reduce_mean(tf.square(y_data - y))
# loss_summary = tf.scalar_summary("loss", loss)

# optimizer = tf.train.GradientDescentOptimizer(0.01)
optimizer = tf.train.AdamOptimizer()
train = optimizer.minimize(loss)

init = tf.initialize_all_variables()

sess = tf.Session()
# merged = tf.merge_all_summaries()
# writer = tf.train.SummaryWriter("/tmp/tensorflow_log", sess.graph_def)
sess.run(init)

for step in xrange(501):
    for i in xrange(data_num):
        if step % 100 == 0:
            sess.run(loss, feed_dict={x_data:x_sample[i], y_data:y_sample[i]})
            result = sess.run(loss,feed_dict={x_data:x_sample[i], y_data:y_sample[i]})
            # summary_str = result[0]
            acc = result
            # writer.add_summary(summary_str, step)
        else:
            sess.run(train, feed_dict={x_data:x_sample[i], y_data:y_sample[i]})
    if step % 100 == 0:
        print step
        print sess.run(w)
        print sess.run(b)
        print acc
        plt.scatter(x_sample, y_sample)

        x=[]
        r = []

        for i in xrange(x_range):
            prot_x = (i-x_range/2.0)*x_plot_sub
            x.append(prot_x)
            r.append(sess.run(y, feed_dict={x_data:[prot_x]}))
            print(r[i])

        plt.plot(x, r, color='orangered')
        plt.pause(2)
        plt.close()

plt.scatter(x_sample, y_sample)

x=[]
r = []

for i in xrange(x_range):
    prot_x = (i-x_range/2.0)*x_plot_sub
    x.append(prot_x)
    r.append(sess.run(y, feed_dict={x_data:[prot_x]}))
    print(r[i])

plt.plot(x, r, color='orangered')

plt.show()

sess.close()

timer = time.time() - start
print(("time:{0}".format(timer)) + "[sec]")