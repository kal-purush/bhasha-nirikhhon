import tensorflow as tf
import random
from util import tf_help
import time


def func_to_approx(x):
    """Function to approximate"""
    if x < 0:
        return 0.0
    else:
        return 1.0


class P:
    """global props so intellij can auto-complete after p.<ctrl+space>"""
    steps = 100
    min_x = -10
    max_x = 10
    number_of_samples = 100
    seed = 123


def makeData():
    features = [random.uniform(P.min_x, P.max_x) for _ in range(P.number_of_samples)]
    labels = [func_to_approx(x) for x in features]
    return (features, labels)


class G:
    """Graph related static ops"""

    print('[Start] Creating graph...')

    features = tf.placeholder(dtype=tf.float32, shape=[None], name="features")
    labels = tf.placeholder(dtype=tf.float32, shape=[None], name="labels")

    with tf.name_scope("model"):
        features_reshaped = tf.reshape(features, shape=[-1, 1], name="features_r")
        predictions = tf.layers.dense(inputs=features_reshaped, units=1, name="predictions")

    with tf.name_scope("loss"):
        labels_reshaped = tf.reshape(labels, shape=[-1, 1], name="labels_r")
        loss = tf.losses.mean_squared_error(labels=labels_reshaped, predictions=predictions)

    with tf.name_scope("training"):
        optimiser = tf.train.GradientDescentOptimizer(1e-5)
        minimise = optimiser.minimize(loss)

    with tf.name_scope("everything_else"):
        init = tf.global_variables_initializer()

    writer = tf_help.crete_file_writer(__file__)
    writer.add_graph(tf.get_default_graph())
    print('[Done] Creating graph')


(xs, ys) = makeData()

with tf.Session() as sess:
    sess.run(G.init)
    for step in range(P.steps):
        _, loss = sess.run((G.minimise, G.loss), feed_dict={G.features: xs, G.labels: ys})
        print("Step %d %s" % (step, loss))

time.sleep(2)  # wait for writer writes events to disk
print("Done")
exit()

import tensorflow as tf

# custom plot
# tp = [] # the true positive rate list
# fp = [] # the false positive rate list
# total = len(fp)
# writer = tf.train.SummaryWriter("/tmp/tensorboard_roc")
# for idx in range(total):
#     summt = tf.Summary()
#     summt.value.add(tag="roc", simple_value = tp[idx])
#     writer.add_summary (summt, tp[idx] * 100) #act as global_step
#     writer.flush ()
#
# then start a tensorboard
# tensorboard --logdir=/tmp/tensorboard_roc
#