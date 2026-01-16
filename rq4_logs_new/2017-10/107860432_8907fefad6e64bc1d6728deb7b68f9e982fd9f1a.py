#coding=utf-8
import numpy as np
import tensorflow as tf
from tensorflow.contrib import rnn
import random
import ProgressBar as pb
from tensorflow.python import control_flow_ops


hop = 71
timestep_size = 71                # Hours of looking ahead
output_parameters = 3   # Number of predicting parameters
num_stations = 3        # Number of monitoring stations


training_epochs = 200
_batch_size = 384


data_dir = "./dev_data/"
def process(x):
    if x == '?':
        return 0.0
    else:
        return float(x)

def batch_norm(x, n_out, phase_train, scope='bn'):
    """
    Batch normalization on convolutional maps.
    Args:
        x:           Tensor, 4D BHWD input maps
        n_out:       integer, depth of input maps
        phase_train: boolean tf.Varialbe, true indicates training phase
        scope:       string, variable scope
    Return:
        normed:      batch-normalized maps
    """
    with tf.variable_scope(scope):
        beta = tf.Variable(tf.constant(0.0, shape=[n_out]),
                                     name='beta', trainable=True)
        gamma = tf.Variable(tf.constant(1.0, shape=[n_out]),
                                      name='gamma', trainable=True)
        batch_mean, batch_var = tf.nn.moments(x, [0,1,2], name='moments')
        ema = tf.train.ExponentialMovingAverage(decay=0.5)

        def mean_var_with_update():
            ema_apply_op = ema.apply([batch_mean, batch_var])
            with tf.control_dependencies([ema_apply_op]):
                return tf.identity(batch_mean), tf.identity(batch_var)

        mean, var = tf.cond(phase_train,
                            mean_var_with_update,
                            lambda: (ema.average(batch_mean), ema.average(batch_var)))
        normed = tf.nn.batch_normalization(x, mean, var, beta, gamma, 1e-3)
    return normed

dataset = []
split = 200
leap = 6
length = 16

lr = 0.0001
hidden_size = 256
layer_num = 3


# from UNIX time 1395691200
# to UNIX time 1448564400
# Read from the file of the training set
data = []
target_set = []

# defines how many hours is used to predict

# --------------------------------------------
#             Data Preparation
# --------------------------------------------

print("Processing target set")
start = 1395691200
end = 1448564400
cur_start = start
cur_end = start+hop*3600
count = 0

bar = pb.ProgressBar(total=(end-start)/3600)
while(cur_end < end-(120+288)*3600):
    bar.move()
    count += 1
    if(count % 100 == 0):
        bar.log('Preparing : ' + str(cur_end) + ' till 1448564400')
    buff = []
    for i in range(hop):
        hour = []
        f1 = open(data_dir+(str)(cur_start+i*3600), 'rb')
        for line in f1.readlines():
            ls = line.split('#')
            hour = hour+(map(float, ls[4:16]))
        f1.close()
        buff.append(hour)
    data.append(buff)
    f1 = open(data_dir+(str)(cur_start+120*3600), 'rb')
    for line in f1.readlines():
        ls = line.split("#")
        target_set.append(map(float, ls[7:10]))
        break
    cur_start = cur_start+3600
    cur_end = cur_end+3600
print(len(target_set))
np_data = np.asarray(data)
np_target = np.asarray(target_set)
print("Target shape :", np_target.shape)
print("Data shape : :", np_data.shape)


X = np_data
y = np_target

training_set = np.array(X[1920:])
training_target = np.array(y[1920:])
val_set = np.array(X[:1920])
val_target = np.array(y[:1920])

sess = tf.InteractiveSession()
batch_size = tf.placeholder(tf.int32)
_X = tf.placeholder(tf.float32, [None, timestep_size, 36])     # TODO change this to the divided ver
y = tf.placeholder(tf.float32, [None, 3])
keep_prob = tf.placeholder(tf.float32)

# --------------------------------------------
#             Construct LSTM cells
# --------------------------------------------

lstm_cell = rnn.LSTMCell(num_units=hidden_size,
                              forget_bias=1.0,
                              state_is_tuple=True)
#                              time_major=False)

lstm_cell = rnn.DropoutWrapper(cell=lstm_cell,
                               input_keep_prob=1.0,
                               output_keep_prob=keep_prob)

mlstm_cell = rnn.MultiRNNCell([lstm_cell] * layer_num, state_is_tuple=True)

init_state = mlstm_cell.zero_state(batch_size, dtype=tf.float32)

outputs, state = tf.nn.dynamic_rnn(mlstm_cell,
                                   inputs=_X,
                                   initial_state=init_state)
h_state = outputs[:, -1, :]  # 或者 h_state = state[-1][1]

# --------------------------------------------
#    Convert LSTM output to tensor of three
# --------------------------------------------
W = tf.Variable(tf.truncated_normal([hidden_size, output_parameters],
                                    stddev=0.1),
                dtype=tf.float32)
bias = tf.Variable(tf.constant(0.1,shape=[output_parameters]),
                   dtype=tf.float32)
y_pre = tf.matmul(h_state, W) + bias

cross_entropy = -tf.reduce_mean(y * tf.log(y_pre))
train_op = tf.train.AdamOptimizer(lr).minimize(cross_entropy)
loss = tf.reduce_mean(tf.abs(y_pre-y),0)

correct_prediction = tf.equal(tf.argmax(y_pre,1), tf.argmax(y,1))
accuracy = tf.reduce_mean(tf.cast(correct_prediction, "float"))


# --------------------------------------------
#               Start Training
# --------------------------------------------

sess.run(tf.global_variables_initializer())
count = 0
for i in range(training_epochs):
    for batch in range(5, 36):
        start = batch*_batch_size
        end = (batch+1)*_batch_size
        sess.run(train_op,
                 feed_dict={_X:data[start:end],
                            y: target_set[start:end],
                            keep_prob: 0.5,
                            batch_size: 384})
    #    print("========Iter:"+str(i)+",Accuracy:========",(acc))
    if(i%3!=0):
        acc = sess.run(loss,
                       feed_dict={_X: data[1152:1536],
                                  y: target_set[1152:1536],
                                  batch_size: 384,
                                  keep_prob: 1})
        print("Epoch:"+str(i)+str(acc))


import math

n_in, n_out = 3, 16
ksize = 3
stride = 1
phase_train = tf.placeholder(tf.bool, name='phase_train')
input_image = tf.placeholder(tf.float32, name='input_image')
kernel = tf.Variable(tf.truncated_normal([ksize, ksize, n_in, n_out],
                                   stddev=math.sqrt(2.0/(ksize*ksize*n_out))),
                                   name='kernel')
conv = tf.nn.conv2d(input_image, kernel, [1,stride,stride,1], padding='SAME')
conv_bn = batch_norm(conv, n_out, phase_train)
relu = tf.nn.relu(conv_bn)

with tf.Session() as session:
    session.run(tf.initialize_all_variables())
    for i in range(20):
        test_image = np.random.rand(4,32,32,3)
        sess_outputs = session.run([relu],
          {input_image.name: test_image, phase_train.name: True})