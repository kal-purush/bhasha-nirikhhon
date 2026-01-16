import numpy as np
import tensorflow as tf
from data import load_data, convert_1d_to_3d, dim_x, dim_y, dim_z

num_labels = 60
num_channels = 1  # grayscale
num_steps = 1001

batch_size = 16
patch_size = 5
depth = 16
num_hidden = 64


def accuracy(predictions, labels):
    return 100.0 * np.sum(np.argmax(predictions, 1) == np.argmax(labels, 1)) / predictions.shape[0]


class ConvolutionalNetwork:
    def __init__(self, image_size_x, image_size_y):
        self.image_size_x = image_size_x
        self.image_size_y = image_size_y

    def reformat(self, dataset):
        """
        Reformat into a TensorFlow-friendly shape:
          - convolutions need the image data formatted as a cube (width by height by #channels)
          - labels as float 1-hot encodings.
        """
        return dataset.reshape((-1, self.image_size_x, self.image_size_y, num_channels)).astype(np.float32)

    def set_datasets(self, dim, data, labels):
        print data.shape
        num_total_data = 360 * 9 * dim
        num_train_offset = num_total_data / 9 * 7
        num_valid_offset = num_train_offset + (num_total_data / 9)
        num_test_offset = num_valid_offset + (num_total_data / 9)

        self.train_dataset = self.reformat(data[0:num_train_offset, :, :])
        self.train_labels = labels[0:num_train_offset, :]
        self.valid_dataset = self.reformat(data[num_train_offset:num_valid_offset, :, :])
        self.valid_labels = labels[num_train_offset:num_valid_offset, :]
        self.test_dataset = self.reformat(data[num_valid_offset:, :, :])
        self.test_labels = labels[num_valid_offset:, :]

        print 'Training set', self.train_dataset.shape, self.train_labels.shape
        print 'Validation set', self.valid_dataset.shape, self.valid_labels.shape
        print 'Test set', self.test_dataset.shape, self.test_labels.shape

    def train(self):
        """
        Two convolutional layers, followed by one fully connected layer, with stride of 2.
        """

        graph = tf.Graph()

        with graph.as_default():
            # Input data.
            tf_train_dataset = tf.placeholder(tf.float32, shape=(batch_size, self.image_size_x, self.image_size_y, num_channels))
            tf_train_labels = tf.placeholder(tf.float32, shape=(batch_size, num_labels))
            tf_valid_dataset = tf.constant(self.valid_dataset)
            tf_test_dataset = tf.constant(self.test_dataset)

            # Variables.
            layer1_weights = tf.Variable(tf.truncated_normal([patch_size, patch_size, num_channels, depth], stddev=0.1))
            layer1_biases = tf.Variable(tf.zeros([depth]))
            layer2_weights = tf.Variable(tf.truncated_normal([patch_size, patch_size, depth, depth], stddev=0.1))
            layer2_biases = tf.Variable(tf.constant(1.0, shape=[depth]))
            layer3_weights = tf.Variable(tf.truncated_normal([self.image_size_x // 4 * self.image_size_y // 4 * depth, num_hidden], stddev=0.1))
            layer3_biases = tf.Variable(tf.constant(1.0, shape=[num_hidden]))
            layer4_weights = tf.Variable(tf.truncated_normal([num_hidden, num_labels], stddev=0.1))
            layer4_biases = tf.Variable(tf.constant(1.0, shape=[num_labels]))

            # Model.
            def model(data):
                conv = tf.nn.conv2d(data, layer1_weights, [1, 2, 2, 1], padding='SAME')
                hidden = tf.nn.relu(conv + layer1_biases)
                conv = tf.nn.conv2d(hidden, layer2_weights, [1, 2, 2, 1], padding='SAME')
                hidden = tf.nn.relu(conv + layer2_biases)
                shape = hidden.get_shape().as_list()
                reshape = tf.reshape(hidden, [shape[0], shape[1] * shape[2] * shape[3]])
                hidden = tf.nn.relu(tf.matmul(reshape, layer3_weights) + layer3_biases)
                return tf.matmul(hidden, layer4_weights) + layer4_biases

            # Training computation.
            logits = model(tf_train_dataset)
            self.loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits, tf_train_labels))

            # Optimizer.
            self.optimizer = tf.train.GradientDescentOptimizer(0.05).minimize(self.loss)

            # Predictions for the training, validation, and test data.
            self.train_prediction = tf.nn.softmax(logits)
            self.valid_prediction = tf.nn.softmax(model(tf_valid_dataset))
            self.test_prediction = tf.nn.softmax(model(tf_test_dataset))

        with tf.Session(graph=graph) as session:
            tf.initialize_all_variables().run()
            print('Initialized')
            for step in range(num_steps):
                offset = (step * batch_size) % (self.train_labels.shape[0] - batch_size)
                batch_data = self.train_dataset[offset:(offset + batch_size), :, :, :]
                batch_labels = self.train_labels[offset:(offset + batch_size), :]
                feed_dict = {tf_train_dataset: batch_data, tf_train_labels: batch_labels}
                _, l, predictions = session.run([self.optimizer, self.loss, self.train_prediction], feed_dict=feed_dict)
                if (step % 50 == 0):
                    print('Minibatch loss at step %d: %f' % (step, l))
                    print('Minibatch accuracy: %.1f%%' % accuracy(predictions, batch_labels))
                    print('Validation accuracy: %.1f%%' % accuracy(self.valid_prediction.eval(), self.valid_labels))
            print('Test accuracy: %.1f%%' % accuracy(self.test_prediction.eval(), self.test_labels))


if __name__ == "__main__":
    data, labels = load_data(True)

    data_dim_x, data_dim_x_label, data_dim_y, data_dim_y_label, data_dim_z, data_dim_z_label = convert_1d_to_3d(data, labels)

    convX = ConvolutionalNetwork(dim_z, dim_y)
    convX.set_datasets(dim_x, data_dim_x, data_dim_x_label)
    #convX.train()