import tensorflow as tf
import time
from util import tf_help

writer = tf_help.crete_file_writer(__file__)
sess = tf.Session()

with tf.name_scope("x_data"):
    x_data = tf.constant([1, 2, 3, 4, 5], dtype=tf.float32)
    x_data = tf.reshape(x_data, [-1, 1])

    x_data_squared = tf.square(x_data)
    x_data = tf.stack([x_data, x_data_squared], -1) #zip
    x_data = tf.reshape(x_data, [-1, 2])
    print(x_data)

with tf.name_scope("y_data"):
    y_data = tf.constant([1, 4, 9, 16, 25], dtype=tf.float32)
    y_data = tf.reshape(y_data, [-1, 1])


model = tf.layers.Dense(
    units=1,
    kernel_initializer=tf.initializers.constant([0.1, 0.2]),
    bias_initializer=tf.initializers.constant(5),
    name="y"
)

y = model(x_data)

perfect_model = tf.layers.Dense(
    units=1,
    kernel_initializer=tf.initializers.constant([0.0, 1.0]),
    bias_initializer=tf.initializers.constant(0),
    name="perfect_y"
)

perfect_y = perfect_model(x_data)

perfect_y_loss = tf.losses.mean_squared_error(labels=y_data, predictions=perfect_y)

loss = tf.losses.mean_squared_error(labels=y_data, predictions=y)
optimizer = tf.train.GradientDescentOptimizer(learning_rate=0.001)
train = optimizer.minimize(loss=loss)

init = tf.global_variables_initializer()
sess.run(init)

for _ in range(10000):
    _, curr_loss, kernel, bias = sess.run((train, loss, model.kernel, model.bias))
    print(curr_loss, kernel, bias)

print("pefrect loss:")
loss, kernel, bias = sess.run((perfect_y_loss, perfect_model.kernel, perfect_model.bias))
print(loss, kernel, bias)


writer.add_graph(tf.get_default_graph())
time.sleep(1)  # wait for writer writes events to disk
print("done")