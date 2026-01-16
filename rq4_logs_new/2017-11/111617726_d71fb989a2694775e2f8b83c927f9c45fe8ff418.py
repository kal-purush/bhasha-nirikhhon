from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import os
import sys
import itertools
import random

import numpy as np
import tensorflow as tf

tf.reset_default_graph()

parser = argparse.ArgumentParser()

# Basic model parameters.
parser.add_argument('--batch_size', type=int, default=1,
                    help='Number of images to process in a batch')

parser.add_argument('--data_dir', type=str, default='/home/cvgl_ros/Documents/parth/',
                    help='Path to the MNIST data directory.')

parser.add_argument('--model_dir', type=str, default='/home/cvgl_ros/Documents/parth/',
                    help='The directory where the model will be stored.')

parser.add_argument('--train_epochs', type=int, default=10,
                    help='Number of epochs to train.')

parser.add_argument(
    '--data_format', type=str, default=None,
    choices=['channels_first', 'channels_last'],
    help='A flag to override the data format used in the model. channels_first '
         'provides a performance boost on GPU but is not always compatible '
         'with CPU. If left unspecified, the data format will be chosen '
         'automatically based on whether TensorFlow was built for CPU or GPU.')

_NUM_IMAGES = {
    'train': 6912/2,
    'validation': 1718/2,
}


test_images_file = np.load("data_images_test.npz")
test_images = test_images_file["arr_0"]
test_images = np.reshape(test_images, [-1, 480, 640, 6])
print(test_images.shape)

test_flows_file = np.load("data_binaries_test.npz")
test_binaries = test_flows_file["arr_0"]
print(test_binaries.shape)

train_images_file = np.load("data_images_train.npz")
train_images = train_images_file["arr_0"]
train_images = np.reshape(train_images, [-1, 480, 640, 6])
print(train_images.shape)

train_flows_file = np.load("data_binaries_train.npz")
train_binaries = train_flows_file["arr_0"]
print(train_binaries.shape)

test_images = test_images / 255.
train_images = train_images / 255.

# sess = tf.Session()

# def gen(images, flows):
#   for i in itertools.count(0):
#     yield images[i], flows[i]

# dataset = tf.data.Dataset.from_generator(
#     gen, (tf.float32, tf.float32))
# value = dataset.make_one_shot_iterator().get_next()

# sess.run(value)
# sess.run(value)

#################################
def input_fn(is_training, images_filename, flows_filename, batch_size=1, num_epochs=1):
  """A simple input_fn using the tf.data input pipeline."""

  def gen():
    for i in itertools.count(0):
      i = i % 3456
      a = random.randint(0,239)
      b = random.randint(0,319)

      image = images_filename[i]
      image = np.expand_dims(image, axis=0)
      image = image[:,a:a+240,b:b+320,:]
      labels = flows_filename[i]
      labels = np.expand_dims(labels, axis=0)
      labels = labels[:,a:a+240,b:b+320,:]
      yield image, labels

  # sess = tf.Session()

  # dataset = tf.data.Dataset.from_tensor_slices(
  #   (images_filename, flows_filename))

  # # Apply dataset transformations
  # if is_training:
  #   # When choosing shuffle buffer sizes, larger sizes result in better
  #   # randomness, while smaller sizes have better performance. Because MNIST is
  #   # a small dataset, we can easily shuffle the full epoch.
  #   dataset = dataset.shuffle(buffer_size=_NUM_IMAGES['train'])

  # # We call repeat after shuffling, rather than before, to prevent separate
  # # epochs from blending together.
  # dataset = dataset.repeat(num_epochs)

  # # Map example_parser over dataset, and batch results by up to batch_size
  # #dataset = dataset.map(example_parser).prefetch(batch_size)
  # dataset = dataset.batch(batch_size)
  # #iterator = dataset.make_one_shot_iterator()
  # iterator = datalset_make_initalizeable_iterator()
  # _images = tf.placeholder(tf.float32, [None, 480, 640, 6])
  # _flows = tf.placeholder(tf.float32, [None, 480, 640, 2])
  # sess.run(iterator.initializer, feed_dict={_data: images_filename,
  #                                           _flows: flows_filename})
  # images, flows = iterator.get_next()

  dataset = tf.data.Dataset.from_generator(
      gen, (tf.float32, tf.float32),
      (tf.TensorShape([1, 240, 320, 6]), tf.TensorShape([1, 240, 320, 2])))
  images, flows =  dataset.make_one_shot_iterator().get_next()

  return images, flows

#################################

def flow_model(inputs, mode, data_format):
  """Takes the image pairs as inputs and mode and outputs a 2D tensor of flows."""
  # Input Layer
  # Reshape X to 4-D tensor: [batch_size, width, height, channels]
  # MNIST images are 28x28 pixels, and have one color channel
  inputs = tf.reshape(inputs, [-1, 240, 320, 6])

  if data_format is None:
    # When running on GPU, transpose the data from channels_last (NHWC) to
    # channels_first (NCHW) to improve performance.
    # See https://www.tensorflow.org/performance/performance_guide#data_formats
    data_format = ('channels_first' if tf.test.is_built_with_cuda() else
                   'channels_last')

  if data_format == 'channels_first':
    inputs = tf.transpose(inputs, [0, 3, 1, 2])

  # Convolutional Layer #1
  # Computes 64 features using a 5x5 filter with stride of 2x2.
  # Activation simulates a leaky ReLU layer.
  # Padding is added to preserve width and height.
  # Input Tensor Shape: [batch_size, 240, 320, 6]
  # Output Tensor Shape: [batch_size, 120, 160, 64]
  conv1 = tf.layers.conv2d(
      inputs=inputs,
      filters=64,
      kernel_size=[5, 5],
      strides=[2, 2],
      padding='same',
      activation=None,
      data_format=data_format)

  
  conv1 = tf.maximum(conv1,tf.scalar_mul(tf.constant(0.1),conv1))


  # Convolutional Layer #2
  # Computes 128 features using a 3x3 filter with stride of 2x2.
  # Activation simulates a leaky ReLU layer.
  # Padding is added to preserve width and height.
  # Input Tensor Shape: [batch_size, 120, 160, 64]
  # Output Tensor Shape: [batch_size, 60, 80, 128]
  conv2 = tf.layers.conv2d(
      inputs=conv1,
      filters=128,
      kernel_size=[3, 3],
      strides=[2, 2],
      padding='same',
      activation=None,
      data_format=data_format)

  conv2 = tf.maximum(conv2,tf.scalar_mul(tf.constant(0.1),conv2))

  # Convolutional Layer #3
  # Computes 256 features using a 3x3 filter with stride of 2x2.
  # Activation simulates a leaky ReLU layer.
  # Padding is added to preserve width and height.
  # Input Tensor Shape: [batch_size, 60, 80, 128]
  # Output Tensor Shape: [batch_size, 30, 40, 256]
  conv3 = tf.layers.conv2d(
      inputs=conv2,
      filters=256,
      kernel_size=[3, 3],
      strides=[2, 2],
      padding='same',
      activation=None,
      data_format=data_format)

  conv3 = tf.maximum(conv3,tf.scalar_mul(tf.constant(0.1),conv3))  

  # Predict Flow #1
  # Computes 2 features using a 3x3 filter with stride of 1x1.
  # No activation layer.
  # Padding is added to preserve width and height.
  # Input Tensor Shape: [batch_size, 30, 40, 256]
  # Output Tensor Shape: [batch_size, 30, 40, 2]
  pred1 = tf.layers.conv2d(
      inputs=conv3,
      filters=2,
      kernel_size=[3, 3],
      strides=[1, 1],
      padding='same',
      activation=None,
      data_format=data_format)

  pred1 = tf.transpose(pred1, [0, 2, 3, 1])

  # Upsample #1
  # Increases image dimensions by double.
  # Input Tensor Shape: [batch_size, 30, 40, 2]
  # Output Tensor Shape: [batch_size, 60, 80, 2]
  upsample1 = tf.image.resize_images(pred1,
  	  [60, 80],
  	  method=tf.image.ResizeMethod.BILINEAR,
  	  align_corners=False)

  upsample1 = tf.transpose(upsample1, [0, 3, 1, 2])

  # Upconvolutional Layer #1
  # Computes 128 features using a 5x5 filter with stride of 2x2.
  # Activation simulates a leaky ReLU layer.
  # Padding is added to preserve width and height.
  # Input Tensor Shape: [batch_size, 30, 40, 256]
  # Output Tensor Shape: [batch_size, 60, 80, 128]
  upconv1 = tf.layers.conv2d_transpose(
  	  inputs=conv3,
  	  filters=128,
  	  kernel_size=[5, 5],
  	  strides=[2, 2],
  	  padding='same',
  	  activation=None,
  	  data_format=data_format)

  upconv1 = tf.maximum(upconv1,tf.scalar_mul(tf.constant(0.1),upconv1))  

  concat1 = tf.concat([upconv1, conv2, upsample1],
  	  1)

  # Predict Flow #2
  # Computes 2 features using a 3x3 filter with stride of 1x1.
  # No activation layer.
  # Padding is added to preserve width and height.
  # Input Tensor Shape: [batch_size, 60, 80, 258]
  # Output Tensor Shape: [batch_size, 60, 80, 2]
  pred2 = tf.layers.conv2d(
      inputs=concat1,
      filters=2,
      kernel_size=[3, 3],
      strides=[1, 1],
      padding='same',
      activation=None,
      data_format=data_format)

  pred2 = tf.transpose(pred2, [0, 2, 3, 1])

  # Upsample #2
  # Increases image dimensions by double.
  # Input Tensor Shape: [batch_size, 60, 80, 2]
  # Output Tensor Shape: [batch_size, 120, 160, 2]
  upsample2 = tf.image.resize_images(pred2,
  	  [120, 160],
  	  method=tf.image.ResizeMethod.BILINEAR,
  	  align_corners=False)

  upsample2 = tf.transpose(upsample2, [0, 3, 1, 2])

  # Upconvolutional Layer #2
  # Computes 64 features using a 5x5 filter with stride of 2x2.
  # Activation simulates a leaky ReLU layer.
  # Padding is added to preserve width and height.
  # Input Tensor Shape: [batch_size, 60, 80, 128]
  # Output Tensor Shape: [batch_size, 120, 160, 64]
  upconv2 = tf.layers.conv2d_transpose(
  	  inputs=upconv1,
  	  filters=128,
  	  kernel_size=[5, 5],
  	  strides=[2, 2],
  	  padding='same',
  	  activation=None,
  	  data_format=data_format)

  upconv2 = tf.maximum(upconv2,tf.scalar_mul(tf.constant(0.1),upconv2))  

  concat2 = tf.concat([upconv2, conv1, upsample2],
  	  1)

  # Predict Flow #3
  # Computes 2 features using a 3x3 filter with stride of 1x1.
  # No activation layer.
  # Padding is added to preserve width and height.
  # Input Tensor Shape: [batch_size, 120, 160, 130]
  # Output Tensor Shape: [batch_size, 120, 160, 2]
  pred3 = tf.layers.conv2d(
      inputs=concat2,
      filters=2,
      kernel_size=[3, 3],
      strides=[1, 1],
      padding='same',
      activation=None,
      data_format=data_format)

  pred3 = tf.transpose(pred3, [0, 2, 3, 1])

  # Upsample #3
  # Increases image dimensions by double.
  # Input Tensor Shape: [batch_size, 120, 160, 2]
  # Output Tensor Shape: [batch_size, 240, 320, 2]
  upsample3 = tf.image.resize_images(pred3,
  	  [240, 320],
  	  method=tf.image.ResizeMethod.BILINEAR,
  	  align_corners=False)


  return upsample3

######### THIS FUNCTION BELOW NEEDS TO BE MODIFIED ######################

# CHANGE PREDICTIONS
# CHANGE LOSS
# CHANGE ACCURACRY AND METRICS? NOT EXACT MATCH BUT CLOSENESS?

def flow_model_fn(features, labels, mode, params):
  """Model function for MNIST."""
  upsample2 = flow_model(features, mode, params['data_format'])

  # predictions = {
  #     'classes': tf.argmax(input=logits, axis=1),
  #     'probabilities': tf.nn.softmax(logits, name='softmax_tensor')
  # }

  predictions = {"results": upsample2[0]}

  if mode == tf.estimator.ModeKeys.PREDICT:
    return tf.estimator.EstimatorSpec(mode=mode, predictions=predictions)

  #loss = tf.losses.softmax_cross_entropy(onehot_labels=labels, logits=logits)
  
  diff = tf.subtract(labels[0], upsample2[0])

  loss = tf.nn.l2_loss(diff)

  # Configure the training op
  if mode == tf.estimator.ModeKeys.TRAIN:
    optimizer = tf.train.AdamOptimizer(learning_rate=1e-4)
    train_op = optimizer.minimize(loss, tf.train.get_or_create_global_step())
  else:
    train_op = None

  # accuracy = tf.metrics.accuracy(
  #     tf.argmax(labels, axis=1), predictions['classes'])
  #  metrics = {'accuracy': accuracy}

  # AEE = tf.scalar_mul(tf.constant(1.0/(640*480)), loss)
  # metrics = {'AEE': AEE}

  accuracy = tf.metrics.mean_absolute_error(labels[0], upsample2[0])
  metrics = {'AEE': accuracy}

  # Create a tensor named train_accuracy for logging purposes
  tf.identity(accuracy[1], name='train_accuracy')
  tf.summary.scalar('train_accuracy', accuracy[1])

  return tf.estimator.EstimatorSpec(
      mode=mode,
      predictions=predictions,
      loss=loss,
      train_op=train_op,
      eval_metric_ops=metrics)

####################################################################

def main(unused_argv):
  # Make sure that training and testing data have been converted.
  # train_file = os.path.join(FLAGS.data_dir, 'data_images_train.txt')
  # test_file = os.path.join(FLAGS.data_dir, 'data_images_test.txt')
  # train_binaries_file = os.path.join(FLAGS.data_dir, 'data_binaries_train.txt')
  # test_binaries_file = os.path.join(FLAGS.data_dir, 'data_binaries_test.txt')
  # assert (tf.gfile.Exists(train_file) and tf.gfile.Exists(test_file)), (
  #     'Run convert_to_records.py first to convert the MNIST data to TFRecord '
  #     'file format.')

  # Create the Estimator
  flow_classifier = tf.estimator.Estimator(
      model_fn=flow_model_fn, model_dir=FLAGS.model_dir,
      params={'data_format': FLAGS.data_format})

  # Set up training hook that logs the training accuracy every 100 steps.
  tensors_to_log = {
      'train_accuracy': 'train_accuracy'
  }
  logging_hook = tf.train.LoggingTensorHook(
      tensors=tensors_to_log, every_n_iter=100)

  ###########FIND A WAY TO ONLY TEST INPUT FN######################
  # LOAD DATA AND PASS IT INTO INPUT FN

  
  # Train the model
  flow_classifier.train(
      input_fn=lambda: input_fn(
          True, train_images, train_binaries, FLAGS.batch_size, FLAGS.train_epochs),
      steps = 3456*10, hooks=[logging_hook])

  # summary_writer = tf.train.SummaryWriter("/tensorflow/logdir", sess.graph_def)

  # Evaluate the model and print results
  eval_results = flow_classifier.evaluate(
      input_fn=lambda: input_fn(False, test_images, test_binaries, FLAGS.batch_size),
      steps = 859)
  print()
  print('Evaluation results:\n\t%s' % eval_results)


if __name__ == '__main__':
  tf.logging.set_verbosity(tf.logging.INFO)
  FLAGS, unparsed = parser.parse_known_args()
  tf.app.run(main=main, argv=[sys.argv[0]] + unparsed)