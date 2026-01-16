import numpy as np
import tensorflow as tf


# sess = tf.InteractiveSession()


# Attention block
def attention_block(hidden_states, state_size, window_size, dim_hlayer, batch_size,
                    activation, L, sketches_num, discount_factor, temperature):

    with tf.variable_scope('loop_matrices', reuse=tf.AUTO_REUSE):
        W_hsz = tf.get_variable(name="W_hsz", shape=[2 * state_size * (2 * window_size + 1), dim_hlayer],
                                initializer=tf.contrib.layers.xavier_initializer(uniform=True,
                                                                                 dtype=tf.float32))

        w_z = tf.get_variable(name="w_z", shape=[dim_hlayer],
                              initializer=tf.random_uniform_initializer(dtype=tf.float32))

        v = tf.get_variable(name="v", shape=[dim_hlayer, 1],
                            initializer=tf.random_uniform_initializer(dtype=tf.float32))

        W_hh = tf.get_variable(name="W_hh", shape=[2 * state_size * (2 * window_size + 1), state_size],
                               initializer=tf.contrib.layers.xavier_initializer(uniform=True, dtype=tf.float32))

        w_h = tf.get_variable(name="w_h", shape=[state_size],
                              initializer=tf.random_uniform_initializer(dtype=tf.float32))

    def conv_r(padded_matrix, r):
        """
        Extract r context columns around each column and concatenate
        :param padded_matrix: batch_size x L+(2*r) x 2*state_size
        :param r: context size
        :return:
        """
        # gather indices of padded
        time_major_matrix = tf.transpose(padded_matrix,
                                         [1, 2, 0])  # time-major  -> L x 2*state_size x batch_size
        contexts = []
        for j in np.arange(r, L + r):
            # extract 2r+1 rows around i for each batch
            context_j = time_major_matrix[j - r:j + r + 1, :, :]  # 2*r+1 x 2*state_size x batch_size
            # concatenate
            context_j = tf.reshape(context_j,
                                   [(2 * r + 1) * 2 * state_size, batch_size])  # (2*r+1)*(state_size) x batch_size
            contexts.append(context_j)
        contexts = tf.stack(contexts)  # L x (2*r+1)* 2*(state_size) x batch_size
        batch_major_contexts = tf.transpose(contexts, [2, 0, 1])
        # switch back: batch_size x L x (2*r+1)*2(state_size) (batch-major)
        return batch_major_contexts

    def prepare_tensor(hidstates, sk, padding_col):
        hs = tf.concat([hidstates, sk], 2)
        # add column on right and left, and add context window
        hs = tf.pad(hs, padding_col, "CONSTANT", name="HS_padded")
        hs = conv_r(hs, window_size)  # [batch_size, L, 2*state*(2*window_size + 1)]
        return hs

    def sketch_step(tensor, cum_attention, temper):

        def constrained_softmax(input_tensor, b, temp):
            """
            Compute the constrained softmax (csoftmax);
            See paper "Learning What's Easy: Fully Differentiable Neural Easy-First Taggers"
            on https://andre-martins.github.io/docs/emnlp2017_final.pdf (page 4)

            :param input_tensor: input tensor
            :param b: cumulative attention see paper
            :param temp: softmax temperature
            :return: distribution
            """

            # input_tensor = tf.reduce_mean(input_tensor)
            z = tf.reduce_sum(tf.exp(input_tensor / temp), axis=1, keep_dims=True)
            a = tf.exp(input_tensor / temp) * (b / temp) / z
            # a = tf.exp(input_tensor/temp) * b / z
            u = tf.ones_like(b) - b
            t_mask = tf.to_float(tf.less_equal(a, u))
            f_mask = tf.to_float(tf.less(u, a))
            A = a * t_mask
            U = u * f_mask

            csoftmax = A + U

            return csoftmax

        def attention(t):
            before_att = activation(tf.matmul(t, W_hsz) + w_z)
            att = tf.matmul(before_att, v)  # [batch_size, 1]
            return att

        tensor = tf.transpose(tensor, [1, 0, 2])

        attentions = tf.map_fn(attention, tensor, dtype=tf.float32)  # [batch_size, 1, L]
        attentions = tf.reshape(attentions, [batch_size, L]) - cum_attention*discount_factor  # [batch_size, L]
        constrained_weights = constrained_softmax(attentions, cum_attention, temper)  # [batch_size, L]

        tensor = tf.transpose(tensor, [1, 0, 2])
        cn = tf.reduce_sum(tensor*tf.expand_dims(constrained_weights, [2]), axis=1)  # [batch_size,
        #  2*state_size*(2*window_size + 1)]
        cn = tf.reshape(cn, [batch_size, 2*state_size*(2*window_size + 1)])  # [batch_size,
        #  2*state_size*(2*window_size + 1)]
        s = activation(tf.matmul(cn, W_hh) + w_h)  # [batch_size, state_size]

        s = tf.matmul(tf.expand_dims(constrained_weights, [2]), tf.expand_dims(s, [1]))  # [batch_size, L,
        #  state_size]

        return s, constrained_weights

    sketch = tf.zeros(shape=[batch_size, L, state_size], dtype=tf.float32)  # sketch tenzor
    cum_att = tf.zeros(shape=[batch_size, L])  # cumulative attention
    padding_hs_col = tf.constant([[0, 0], [window_size, window_size], [0, 0]], name="padding_hs_col")
    temperature = tf.constant(temperature, dtype=tf.float32, name='attention_temperature')
    sketches = []
    cum_attentions = []

    for i in range(sketches_num):
        sketch_, cum_att_ = sketch_step(prepare_tensor(hidden_states, sketch, padding_hs_col), cum_att,
                                        temperature)
        # print(cum_att_.eval(session=sess))
        sketch += sketch_
        cum_att += cum_att_
        sketches.append(sketch_)  # list of tensors with shape [batch_size, L, state_size]
        cum_attentions.append(cum_att_)  # list of tensors with shape [batch_size, L]

    return sketches, cum_attentions


hidden_states = tf.placeholder(dtype=tf.float32, shape=[100, 83, 40])
state_size = 40
dim_hlayer = 20
window_size = 2
batch_size = 100
activation = tf.nn.tanh
L = 83
sketches_num = 5
discount_factor = 0
temperature = 1

input_tensor = np.random.randn(batch_size, L, state_size)
# print(input_tensor)

sketch_list, cum_attention = attention_block(hidden_states, state_size, window_size, dim_hlayer, batch_size,
                                             activation, L, sketches_num, discount_factor, temperature)

with tf.Session() as sess:
    sess.run(tf.global_variables_initializer())

    sketchs, cum_att = sess.run([sketch_list, cum_attention], feed_dict={hidden_states: input_tensor})

    print(cum_att)
    print(sketchs[0].shape)
    print(len(sketchs))
