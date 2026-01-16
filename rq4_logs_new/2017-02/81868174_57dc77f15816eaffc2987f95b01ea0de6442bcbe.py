from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

## based on this code:

########################################################################################
# Davi Frossard, 2016                                                                  #
# VGG16 implementation in TensorFlow                                                   #
# Details:                                                                             #
# http://www.cs.toronto.edu/~frossard/post/vgg16/                                      #
#                                                                                      #
# Model from https://gist.github.com/ksimonyan/211839e770f7b538e2d8#file-readme-md     #
# Weights from Caffe converted using https://github.com/ethereon/caffe-tensorflow      #
########################################################################################

import os
import sys
import tensorflow as tf
import numpy as np
from scipy.misc import imresize
import h5py

class vgg16:
    def __init__(self, imgs, weights=None, sess=None, trainable=False, stop_at_fc2=False):
        self.imgs = imgs
        self.stop_at_fc2=stop_at_fc2
        self.trainable=trainable
        self.after_relus = []
        self._gbprop_pool5_op = None
        self._saliency_pool5_op = None
        self._pl_pool5 = tf.placeholder(dtype=tf.float32, shape=(None,7,7,512))
        
        self.convlayers(trainable)
        self.fc_layers(trainable)
        if not self.stop_at_fc2:
            self.probs = tf.nn.softmax(self.fc3l)
        if weights is not None and sess is not None:
            self.load_weights(weights, sess)
        self.layer_name_to_op = {'conv1_1':self.conv1_1,
                                 'conv1_2':self.conv1_2,
                                 'pool1':self.pool1,
                                 'conv2_1':self.conv2_1,
                                 'conv2_2':self.conv2_2,
                                 'pool2':self.pool2,
                                 'conv3_1':self.conv3_1,
                                 'conv3_2':self.conv3_2,
                                 'conv3_3':self.conv3_3,
                                 'pool3':self.pool3,
                                 'conv4_1':self.conv4_1,
                                 'conv4_2':self.conv4_2,
                                 'conv4_3':self.conv4_3,
                                 'pool4':self.pool4,
                                 'conv5_1':self.conv5_1,
                                 'conv5_2':self.conv5_2,
                                 'conv5_3':self.conv5_3,
                                 'pool5':self.pool5,
                                 'fc1':self.fc1,
                                 'fc2':self.fc2}
                                 

    def convlayers(self, trainable):
        self.parameters = []

        images = self.imgs

        # conv1_1
        with tf.name_scope('conv1_1') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 3, 64], dtype=tf.float32,
                                                     stddev=1e-1), name='weights',
                                 trainable=trainable)
            conv = tf.nn.conv2d(images, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv1_1 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[64], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv1_1 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv1_1)
            self.parameters += [kernel, biases]

        # conv1_2
        with tf.name_scope('conv1_2') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 64, 64], dtype=tf.float32,
                                                     stddev=1e-1), name='weights',
                                 trainable=trainable)
            conv = tf.nn.conv2d(self.conv1_1, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv1_2 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[64], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv1_2 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv1_2)
            self.parameters += [kernel, biases]

        # pool1
        self.pool1 = tf.nn.max_pool(self.conv1_2,
                               ksize=[1, 2, 2, 1],
                               strides=[1, 2, 2, 1],
                               padding='SAME',
                               name='pool1')

        # conv2_1
        with tf.name_scope('conv2_1') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 64, 128], dtype=tf.float32,
                                                     stddev=1e-1), name='weights',
                                             trainable=trainable)
            conv = tf.nn.conv2d(self.pool1, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv2_1 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[128], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv2_1 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv2_1)
            self.parameters += [kernel, biases]

        # conv2_2
        with tf.name_scope('conv2_2') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 128, 128], dtype=tf.float32,
                                                     stddev=1e-1), name='weights',
                                                                  trainable=trainable)
            conv = tf.nn.conv2d(self.conv2_1, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv2_2 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[128], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv2_2 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv2_2)
            self.parameters += [kernel, biases]

        # pool2
        self.pool2 = tf.nn.max_pool(self.conv2_2,
                               ksize=[1, 2, 2, 1],
                               strides=[1, 2, 2, 1],
                               padding='SAME',
                               name='pool2')

        # conv3_1
        with tf.name_scope('conv3_1') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 128, 256], dtype=tf.float32,
                                                     stddev=1e-1), name='weights',
                                 trainable=trainable)

            conv = tf.nn.conv2d(self.pool2, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv3_1 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[256], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv3_1 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv3_1)
            self.parameters += [kernel, biases]

        # conv3_2
        with tf.name_scope('conv3_2') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 256, 256], dtype=tf.float32,
                                                     stddev=1e-1), name='weights',
                                 trainable=trainable)
            conv = tf.nn.conv2d(self.conv3_1, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv3_2 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[256], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv3_2 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv3_2)
            self.parameters += [kernel, biases]

        # conv3_3
        with tf.name_scope('conv3_3') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 256, 256], dtype=tf.float32,
                                                     stddev=1e-1), name='weights',
                                 trainable=trainable)
            conv = tf.nn.conv2d(self.conv3_2, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv3_3 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[256], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv3_3 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv3_3)
            self.parameters += [kernel, biases]

        # pool3
        self.pool3 = tf.nn.max_pool(self.conv3_3,
                               ksize=[1, 2, 2, 1],
                               strides=[1, 2, 2, 1],
                               padding='SAME',
                               name='pool3')

        # conv4_1
        with tf.name_scope('conv4_1') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 256, 512], dtype=tf.float32,
                                                     stddev=1e-1), name='weights', trainable=trainable)
            conv = tf.nn.conv2d(self.pool3, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv4_1 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[512], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv4_1 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv4_1)
            self.parameters += [kernel, biases]

        # conv4_2
        with tf.name_scope('conv4_2') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 512, 512], dtype=tf.float32,
                                                     stddev=1e-1), name='weights', trainable=trainable)
            conv = tf.nn.conv2d(self.conv4_1, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv4_2 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[512], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv4_2 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv4_2)
            self.parameters += [kernel, biases]

        # conv4_3
        with tf.name_scope('conv4_3') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 512, 512], dtype=tf.float32,
                                                     stddev=1e-1), name='weights', trainable=trainable)
            conv = tf.nn.conv2d(self.conv4_2, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv4_3 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[512], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv4_3 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv4_3)
            self.parameters += [kernel, biases]

        # pool4
        self.pool4 = tf.nn.max_pool(self.conv4_3,
                               ksize=[1, 2, 2, 1],
                               strides=[1, 2, 2, 1],
                               padding='SAME',
                               name='pool4')

        # conv5_1
        with tf.name_scope('conv5_1') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 512, 512], dtype=tf.float32,
                                                     stddev=1e-1), name='weights', trainable=trainable)
            conv = tf.nn.conv2d(self.pool4, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv5_1 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[512], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv5_1 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv5_1)
            self.parameters += [kernel, biases]

        # conv5_2
        with tf.name_scope('conv5_2') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 512, 512], dtype=tf.float32,
                                                     stddev=1e-1), name='weights', trainable=trainable)
            conv = tf.nn.conv2d(self.conv5_1, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv5_2 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[512], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv5_2 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv5_2)
            self.parameters += [kernel, biases]

        # conv5_3
        with tf.name_scope('conv5_3') as scope:
            kernel = tf.Variable(tf.truncated_normal([3, 3, 512, 512], dtype=tf.float32,
                                                     stddev=1e-1), name='weights', trainable=trainable)
            conv = tf.nn.conv2d(self.conv5_2, kernel, [1, 1, 1, 1], padding='SAME')
            self.conv_input_to_conv5_3 = conv
            biases = tf.Variable(tf.constant(0.0, shape=[512], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            out = tf.nn.bias_add(conv, biases)
            self.conv5_3 = tf.nn.relu(out, name=scope)
            self.after_relus.append(self.conv5_3)
            self.parameters += [kernel, biases]

        # pool5
        self.pool5 = tf.nn.max_pool(self.conv5_3,
                               ksize=[1, 2, 2, 1],
                               strides=[1, 2, 2, 1],
                               padding='SAME',
                               name='pool4')

    def fc_layers(self, trainable):
        # fc1
        with tf.name_scope('fc1') as scope:
            shape = int(np.prod(self.pool5.get_shape()[1:]))
            fc1w = tf.Variable(tf.truncated_normal([shape, 4096],
                                                         dtype=tf.float32,
                                                         stddev=1e-1), name='weights', trainable=trainable)
            fc1b = tf.Variable(tf.constant(1.0, shape=[4096], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            pool5_flat = tf.reshape(self.pool5, [-1, shape])
            fc1l = tf.nn.bias_add(tf.matmul(pool5_flat, fc1w), fc1b)
            self.fc1 = tf.nn.relu(fc1l)
            self.after_relus.append(self.fc1)
            self.parameters += [fc1w, fc1b]

        # fc2
        with tf.name_scope('fc2') as scope:
            fc2w = tf.Variable(tf.truncated_normal([4096, 4096],
                                                         dtype=tf.float32,
                                                         stddev=1e-1), name='weights', trainable=trainable)
            fc2b = tf.Variable(tf.constant(1.0, shape=[4096], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            fc2l = tf.nn.bias_add(tf.matmul(self.fc1, fc2w), fc2b)
            self.fc2 = tf.nn.relu(fc2l)
            self.after_relus.append(self.fc2)

            self.parameters += [fc2w, fc2b]

        if self.stop_at_fc2:
            return
        
        # fc3
        with tf.name_scope('fc3') as scope:
            fc3w = tf.Variable(tf.truncated_normal([4096, 1000],
                                                         dtype=tf.float32,
                                                         stddev=1e-1), name='weights', trainable=trainable)
            fc3b = tf.Variable(tf.constant(1.0, shape=[1000], dtype=tf.float32),
                                 trainable=trainable, name='biases')
            self.fc3l = tf.nn.bias_add(tf.matmul(self.fc2, fc3w), fc3b)
            self.parameters += [fc3w, fc3b]

    def load_weights(self, weight_file, sess):
        weights = np.load(weight_file)
        keys = sorted(weights.keys())
        for i, k in enumerate(keys):
            if self.stop_at_fc2 and i >= 30:
                print("hit fc3 - exiting load weights")
                break
            print( i, k, np.shape(weights[k]))
            sess.run(self.parameters[i].assign(weights[k]))

    def get_W_B(self, name):
        assert name in ['fc2','fc1']
        if name == 'fc2':
            W = self.parameters[-4].eval()
            B = self.parameters[-3].eval()
            assert self.parameters[-4].name.startswith('fc2/weights')
            assert self.parameters[-3].name.startswith('fc2/biases')
        elif name == 'fc1':
            W = self.parameters[-6].eval()
            B = self.parameters[-5].eval()
            assert self.parameters[-6].name.startswith('fc1/weights')
            assert self.parameters[-5].name.startswith('fc1/biases')
        return W,B
        
    def get_model_layers(self, sess, imgs, layer_names):
        ops = [self.layer_name_to_op[name] for name in layer_names]
        arrs = sess.run(ops, feed_dict={self.imgs:imgs})
        return arrs

    def relevance_propagation(self, Rel_fc2):
        '''returns 
        '''
        pass
    
    def gbprop_op_pool5(self):
        if self._gbprop_pool5_op is None:
            relus = [op for op in self.after_relus]
            relus.pop()  # fc2
            relus.pop()  # fc1

            ## check that it is the relu before the max pooling for pool5
            assert relus[-1].get_shape().as_list()==[None, 14,14,512], "whoops! relus[-1]=%s does not have shape [None,14,14,512]" % self.relus[-1]
        
            yy = self.pool5
            grad_ys = self._pl_pool5

            while len(relus):
                xx = relus.pop()
                dyy_xx = tf.gradients(ys=yy, xs=xx, grad_ys=grad_ys)[0]
                grad_ys = tf.nn.relu(dyy_xx)
                print(grad_ys)
                print(xx)
                print(dyy_xx)
                print(yy)
                yy = xx
            self._gbprop_pool5_op = tf.gradients(ys=yy, xs=self.imgs, grad_ys=grad_ys)[0]
        return self._gbprop_pool5_op, self._pl_pool5

    def saliency_op_pool5(self):
        if self._saliency_pool5_op is None:
            op = tf.gradients(ys=self.pool5, xs=self.imgs, grad_ys=self._pl_pool5)[0]
            print(op)
            self._saliency_pool5_op = op
        return self._saliency_pool5_op, self._pl_pool5
    

def create(session, weights):
    imgs = tf.placeholder(tf.float32, [None, 224, 224, 3])
    return vgg16(imgs=imgs, weights=weights, sess=session)