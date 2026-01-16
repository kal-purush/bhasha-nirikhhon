import os
import tensorflow as tf

save_path = os.getcwd() + "/tmp"
tf.reset_default_graph()
with tf.Session() as sess:
    saver = tf.train.import_meta_graph(save_path + '/model.ckpt.meta')
    saver.restore(sess, tf.train.latest_checkpoint(save_path))
    sess.run(tf.global_variables_initializer())
    all_vars = tf.trainable_variables()
    for v in all_vars:
        print("%s with value %s" % (v.name, sess.run(v)))