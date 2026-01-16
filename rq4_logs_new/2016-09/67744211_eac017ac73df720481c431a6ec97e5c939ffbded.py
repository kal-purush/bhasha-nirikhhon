import tensorflow as tf
sess = tf.InteractiveSession()

from tensorflow.examples.tutorials.mnist import input_data
mnist = input_data.read_data_sets('MNIST_data', one_hot=True)

# initializes place holders
x = tf.placeholder(tf.float32, shape=[None, 784])
y_ = tf.placeholder(tf.float32, shape=[None, 10])

# initializes 'Variable' to have the dimensions desired
W = tf.Variable(tf.zeros([784,10]))
b = tf.Variable(tf.zeros([10]))

# start session
sess.run(tf.initialize_all_variables())

# matrix multiplication
y = tf.nn.softmax(tf.matmul(x,W) + b)
# help print out y
y = tf.Print(y, [y], "prediction")

# cross_entropy tells us how bad the model is doing
cross_entropy = tf.reduce_mean(-tf.reduce_sum(y_ * tf.log(y), reduction_indices=[1]))

# optimize the model by minimizing the cross entropy
train_step = tf.train.GradientDescentOptimizer(0.5).minimize(cross_entropy)

# feeding in training data
for i in range(1000):
  batch = mnist.train.next_batch(50)
  #print('img is', batch[0][0])
  train_step.run(feed_dict={x: batch[0], y_: batch[1]})

# calculate the correct predictions
correct_prediction = tf.equal(tf.argmax(y,1), tf.argmax(y_,1))

# calculate the accuracy
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

# output the accuracy
print(accuracy.eval(feed_dict={x: mnist.test.images, y_: mnist.test.labels}))