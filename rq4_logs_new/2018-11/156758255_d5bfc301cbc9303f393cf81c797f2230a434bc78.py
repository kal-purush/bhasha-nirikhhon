import tensorflow as tf
import edward as ed

from edward.models import Gamma, Poisson, PointMass


def pointmass_q(shape, name=None):
    with tf.variable_scope(name, default_name="pointmass_q"):
        min_mean = 1e-3
        mean = tf.get_variable("mean", shape)
        rv = PointMass(tf.maximum(tf.nn.softplus(mean), min_mean))
        return rv

def gamma_q(shape, name=None):
  # Parameterize Gamma q's via shape and scale, with softplus unconstraints.
    with tf.variable_scope(name, default_name="gamma_q"):
        min_shape = 1e-3
        min_scale = 1e-5
        shape_var = tf.get_variable(
            "shape", shape,
            initializer=tf.random_normal_initializer(mean=0.5, stddev=0.1))
        scale_var = tf.get_variable(
            "scale", shape,
            initializer=tf.random_normal_initializer(stddev=0.1))
        rv = Gamma(tf.maximum(tf.nn.softplus(shape_var), min_shape),
                   tf.maximum(1.0 / tf.nn.softplus(scale_var), 1.0 / min_scale))
        return rv