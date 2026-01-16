#coding=utf-8
import numpy as np
import tensorflow as tf
from tensorflow.contrib import rnn
import random
import ProgressBar as pb


hop = 71
timestep_size = 71                # Hours of looking ahead
output_parameters = 3   # Number of predicting parameters
num_stations = 3        # Number of monitoring stations


data_dir = "./dev_data/"
def process(x):
    if x == '?':
        return 0.0
    else:
        return float(x)

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
_X = tf.placeholder( )      # TODO：Add here
y = tf.placeholder( )       # TODO: Add here

# TODO: --------------------------------------------
# TODO:       Construct MLP computation graph
# TODO: --------------------------------------------



# TODO: --------------------------------------------
# TODO:          Construct Training Algo
# TODO: --------------------------------------------
W = tf.Variable(tf.truncated_normal([hidden_size, output_parameters],
                                    stddev=0.1),
                dtype=tf.float32)
bias = tf.Variable(tf.constant(0.1, shape=[output_parameters]),
                   dtype=tf.float32)
y_pre = tf.matmul(_X, W) + bias

cross_entropy = -tf.reduce_mean(y * tf.log(y_pre))
train_op = tf.train.AdamOptimizer(lr).minimize(cross_entropy)
loss = tf.reduce_mean(tf.abs(y_pre-y), 0)

# --------------------------------------------
#               Start Training
# --------------------------------------------
sess.run(tf.global_variables_initializer())
count = 0
for i in range(6000):
    _batch_size = 384
    batch = random.randint(5, 36)
    start = batch*_batch_size
    end = (batch+1)*_batch_size
    sess.run(train_op,
             feed_dict={_X:data[start:end],
                        y: target_set[start:end],
                        batch_size: 384})
    if not (i % 20):
        acc = sess.run(loss,
                       feed_dict={_X: data[1152:1536],
                                  y: target_set[1152:1536],
                                  batch_size: 384})
        print("Epoch:"+str(count)+str(acc))
        count = count+1