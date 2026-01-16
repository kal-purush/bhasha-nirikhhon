"""deuNet Demo of AutoEncoder on MNIST"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import numpy as np
import tensorflow as tf

from IPython import embed

ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(1,ROOT)

import deuNet
import Dataset

class HParams(object):
    pass


def make_loss(model):
    loss = tf.reduce_mean(tf.nn.sigmoid_cross_entropy_with_logits(model.outputs["decoded"],model.tracables["flat_x"]))
    model.tracables.update({"loss":loss})
    return model


def model_builder(in_shape):
    """Build a 3 Layer AutoEncoder Network"""
    encoding_dim1, encoding_dim2, encoding_dim = 128, 64, 32
    
    in_x = tf.placeholder(tf.float32, in_shape)
    flat_x = deuNet.BatchFlatten("input_flatten")(in_x)

    # Encoding
    with tf.variable_scope("Encodding"):
        encoded = deuNet.Dense(encoding_dim1, activation="relu", initializer="he_normal",name="dense1")(flat_x)
        encoded = deuNet.Dense(encoding_dim2, activation="relu", initializer="he_normal",name="dense2")(encoded)
        encoded = deuNet.Dense(encoding_dim, activation="relu", initializer="he_normal",name="dense3")(encoded)
        
    # Decoding
    with tf.variable_scope("Decodding"):
        decoded = deuNet.Dense(encoding_dim2, activation="relu", initializer="he_normal",name="dense1")(encoded)
        decoded = deuNet.Dense(encoding_dim1, activation="relu", initializer="he_normal",name="dense2")(decoded)
        decoded = deuNet.Dense(28*28, activation="sigmoid", initializer="he_normal",name="dense3")(decoded)
    
    # Make End-To-End Model
    _inputs = {"in_x":in_x}
    _outputs = {"decoded":decoded}
    model = deuNet.Model(_inputs, _outputs)
    model.tracables.update({"flat_x":flat_x})
    model = make_loss(model)

    # Make Encoder Model
    _inputs = {"in_x":in_x}
    _outputs = {"encoded":encoded}
    encoder_model = deuNet.Model(_inputs, _outputs)
    
    # Make Decoder Model
    latent = tf.placeholder(tf.float32, (None,encoding_dim),name="latent")
    _inputs = {"latent":latent}
    _outputs = {"encoded":encoded}
    decoder_model = deuNet.Model(_inputs, _outputs)

    return model, encoder_model, decoder_model


def process_epoch(sess, model, data, train_mode=False):
    """Process training/testing process for each epoch through multiple batchs"""
    num_examples = data.num_examples
    batch_size = model.params.batch_size
    total_batch = num_examples // batch_size
    if not train_mode:
        total_batch = 1 # evaluate the full batch once
    
    avg_loss = 0.
    for i in range(total_batch):
        if train_mode:
            batch_images, batch_labels = data.next_batch(batch_size,one_hot=True)
        else:
            batch_images, batch_labels = data.next_batch(batch_size,one_hot=True,full_batch=True)
        feed_dict = {model.inputs["in_x"]:batch_images, model.learning_rate:model.params.current_lr} 
        fetch_dict = model.tracables.copy()
        if train_mode:
            fetch_dict.update({"train_step":model.train_step})
        fetch_vals = deuNet.tf_run_sess(sess, fetch_dict, feed_dict)
        avg_loss += fetch_vals["loss"]
    avg_loss /= total_batch
    return avg_loss


def train(mnist, params):
    model,encoder_model, decoder_model = model_builder(params.input_dims)
    model.params = params
    model.params.current_lr = params.lr

    update_weights = model.trainable_variables.values()
    model.learning_rate = tf.placeholder(tf.float32, shape=[])

    opt = tf.train.AdamOptimizer(model.learning_rate)
    grads_and_vars = opt.compute_gradients(model.tracables["loss"])
    model.train_step = opt.apply_gradients(grads_and_vars)

    init = tf.global_variables_initializer()
    sess = tf.Session()
    sess.run(init)

    print("Training Starts...\n")
    for epoch in range(params.n_epochs):
        eval_loss = process_epoch(sess, model, mnist.valid, train_mode=False)
        train_loss = process_epoch(sess, model, mnist.train, train_mode=True)
        test_loss = process_epoch(sess, model, mnist.test, train_mode=False)
        print(deuNet.color_string("On epoch {}, validation loss = {}".format(epoch, eval_loss),'OKBLUE'))
        print(deuNet.color_string("On epoch {}, training loss = {}".format(epoch, train_loss),'OKGREEN'))
        print(deuNet.color_string("On epoch {}, testing loss = {}".format(epoch, test_loss),'FAIL'))
        print("")
        model.params.current_lr *= model.params.lr_decay
    
        
def get_params():
    params = HParams()
    params.lr = 1e-2
    params.lr_decay = .99
    params.input_dims = [None, 28, 28, 1]
    params.label_dims = [None,10]
    params.batch_size = 100
    params.n_epochs = 10
    params.eval_interval = 10
    return params

if __name__ == '__main__':
    params = get_params()
    mnist = Dataset.read_data_sets()
    train(mnist, params)