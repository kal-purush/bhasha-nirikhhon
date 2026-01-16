import time
import numpy as np 
import tensorflow as tf

_GRUCell = tf.contrib.rnn.GRUCell
_BiDRNN = tf.nn.bidirectional_dynamic_rnn
_Loss = tf.nn.sigmoid_cross_entropy_with_logits
_Optz = tf.train.AdamOptimizer

_EMBEDDING_DIM = 100    # dim for embedding layer
_NUM_UNITS = 150        # size for BiRNN Cell
_ATTENTION_DIM = 50     # dim of attention weight
_LEARNING_RATE = 1e-3   # learning_rate
_BATCH_SIZE = 256
_EPOCHS = 3             # for word embed pre-train
_MAX_ITERATION = 20
_KEEP_PROB = 0.8
_DELTA = 0.5
_MODEL_PATH = './models/'
_MODEL_NAME = _MODEL_PATH+"model-han"



# Hierarchical Attention Networks 
class HAN(object):
    '''
        The idea was proposed in the article by Z. Yang et al., 
        "Hierarchical Attention Networks for Document Classification", 2016: 
        http://www.aclweb.org/anthology/N16-1174.
    '''
    def __init__(self, seq_lens, vocab_size,
                 num_units = _NUM_UNITS, 
                 embedding_dim = _EMBEDDING_DIM,
                 attention_dim = _ATTENTION_DIM,
                 time_major = False,        
                 return_alpha = False,
                 batch_size = _BATCH_SIZE,
                 learning_rate = _LEARNING_RATE):
                 
        start = time.time()
        self.batch_size = batch_size

        with tf.name_scope('init'):
            self.inputs = tf.placeholder(tf.int32, [None, seq_lens], name='inputs')
            self.target = tf.placeholder(tf.float32, [None], name='target')
            self.seq_len = tf.placeholder(tf.int32, [None], name='seq_len')
            self.keep_prob = tf.placeholder(tf.float32, name='keep_prob')
        
        # build model
        print('build model start ....')
        with tf.name_scope('embedding_layer'):
            embed_var = tf.Variable(tf.random_uniform([vocab_size, embedding_dim], -1.0, 1.0))
            embeded = tf.nn.embedding_lookup(embed_var, self.inputs)
        
        with tf.name_scope('Bi_RNN'):
            fw, bw = _GRUCell(num_units), _GRUCell(num_units)
            opts, _ = _BiDRNN(fw, bw, inputs=embeded, 
                              sequence_length=self.seq_len, dtype=tf.float32)

        with tf.name_scope('attention_layer'):
            attention_opt = self.attention(opts, attention_dim, time_major, return_alpha)

        with tf.name_scope('full_connect_layer'):
            # notice W for two gru cell forward and backand
            W = tf.Variable(tf.truncated_normal([_NUM_UNITS*2, 1], stddev=0.1))
            b = tf.Variable(tf.constant(0., shape=[1]))

            drop = tf.nn.dropout(attention_opt, self.keep_prob)
            y_hat = tf.squeeze(tf.nn.xw_plus_b(drop, W, b))
        
        with tf.name_scope('train_op'):
            self.loss = tf.reduce_mean(_Loss(logits=y_hat, labels=self.target))
            self.optimizer = _Optz(learning_rate=learning_rate).minimize(self.loss)

            predict = tf.equal(tf.round(tf.sigmoid(y_hat)),self.target)
            self.accuracy = tf.reduce_mean(tf.cast(predict, tf.float32))
        self.saver = tf.train.Saver()
        print('build model finished, cost: {}'.format(time.time() - start))
    
    def attention(self, inputs, attention_size, time_major, return_alpha):
        '''
            inputs: attention layer input from output state of Bi-RNN,
                    Bi-RNN's output state should be a tuple : (forward, backword)
            attention_size: attention parameters weight size
            time_major: The shape format of the inputs Tensors.
                        If true, [max_time, batch_size, depth].
                        If false, [batch_size, max_time, depth]
                Using `time_major = True` is a bit more efficient because it 
                avoids transposes at the beginning and end of the RNN calculation.  
                However, most TensorFlow data is batch-major, so by default 
                this function accepts input and emits output in batch-major form.
            return_alpha: whether to return weight [alpha] of output state of Bi-RNN
        '''
        if isinstance(inputs, tuple):
            inputs = tf.concat(inputs, 2)
        
        if time_major:
            # shape (T, B, Dim) = (B, T, Dim)
            inputs = tf.array_ops.transpose(inputs, [1, 0, 2])
        
        hidden_size = inputs.shape[2].value # RNN hidden size (Dim)

        # trainable variable
        # w, b : parameter for calculate score by tanh(w*h+b)  
        # u : parameter for calculate alpha by softmax(u * score)
        # output context : reduce_sum(inputs * alpha)
        w = tf.Variable(tf.random_normal([hidden_size, attention_size], stddev=0.1))
        b = tf.Variable(tf.random_normal([attention_size], stddev=0.1))
        u = tf.Variable(tf.random_normal([attention_size], stddev=0.1))

        with tf.name_scope('v'):
            score = tf.tanh(tf.tensordot(inputs, w, axes=1) + b)    # (B, T, Adim)
        alpha = tf.nn.softmax(tf.tensordot(score, u, axes=1, name='vu'), name='alpha') # (B, T)
        context = tf.reduce_sum(inputs*tf.expand_dims(alpha, axis=-1), axis=1)  # (B, Dim)

        if return_alpha:
            return context, alpha
        return context
    
    def init_sess_config(self, allow_growth=True, gpu_memory_frac=1.0):
        '''
            allow_growth: if True, gpu memory will increase by requirement 
            gpu_memory_frac: percentage of gpu memory use
        '''
        options = tf.GPUOptions(allow_growth=allow_growth,  
                                per_process_gpu_memory_fraction=gpu_memory_frac)
        return tf.ConfigProto(gpu_options=options)

    def get_batch(self, x, y, max_iteration=_MAX_ITERATION):

        '''
            max_iteration: control loop times, if not 0, else loop always
        '''
        
        def _shuffle(x, y):
            size = x.shape[0]
            indices = np.arange(size)
            np.random.shuffle(indices)
            x_copy = x.copy()[indices]
            y_copy = y.copy()[indices]
            return x_copy, y_copy
        
        xc, yc = _shuffle(x, y)
        size = x.shape[0]
        
        i = 0
        minus = 1 if max_iteration else 0
        while max_iteration >= 0:
            max_iteration -= minus
            if i + self.batch_size <= size:
                yield xc[i:i+self.batch_size], yc[i:i+self.batch_size]
                i += self.batch_size
            else:       # re-shuffle data x and y, then re-generate batches
                i = 0
                xc, yc = _shuffle(x, y)
                continue

    def reload_model(self, sess):
        # load checkpoint including all structure
        checkpoint = tf.train.get_checkpoint_state(_MODEL_PATH)     
        if checkpoint and checkpoint.model_checkpoint_path:                 
            # load trained network
            self.saver.restore(sess, checkpoint.model_checkpoint_path)
            print ("Successfully loaded:", checkpoint.model_checkpoint_path)
        else:
            print ("no old network weights can be loaded")

    def training(self, x_tr, y_tr, 
                 epochs = _EPOCHS, 
                 keep_prob = _KEEP_PROB,
                 delta = _DELTA,
                 sess_conf = None):

        tr_batch_gen = self.get_batch(x_tr, y_tr, max_iteration=0)

        if sess_conf is None:
            sess_conf = self.init_sess_config()

        with tf.Session(config=sess_conf) as sess:
            
            self.reload_model(sess)

            sess.run(tf.global_variables_initializer())
            print('Training start ....')

            for epoch_i in range(epochs):
                loss_tr = 0
                num_batches = x_tr.shape[0]//self.batch_size

                for b in range(num_batches):
                    start = time.time()
                    x_batch, y_batch = next(tr_batch_gen)
                    # seq_len = np.array([list(x).index(0) + 1 for x in x_batch])
                    seq_len = np.array([len(list(x)) for x in x_batch])
                    loss, acc, _ = sess.run(
                        [self.loss, self.accuracy, self.optimizer],
                        feed_dict={
                            self.inputs: x_batch,
                            self.target: y_batch,
                            self.seq_len: seq_len,
                            self.keep_prob: keep_prob
                        }
                    )
                    loss_tr = loss * delta + loss_tr * (1 - delta)
                    period = time.time() - start
                    start += period 
                    print('Training: Epoch {} Batch {}/{} - Period: {}'
                        .format(epoch_i, b, num_batches, period))
                    print('Loss: {} - Accuracy: {} '.format(loss_tr, acc))
                # save epoch i
                print('save sub_epoch_{} model'.format(epoch_i+1))
                self.saver.save(sess, _MODEL_NAME, global_step=((epoch_i+1)*num_batches))

            # Save final Model
            print('Model Trained and Saved')
            self.saver.save(sess, _MODEL_NAME, global_step=(epochs*num_batches))
            print('Training {} epoachs finished'.format(epochs))
    
    def test(self, x_ts, y_ts, 
             epochs = 1, 
             keep_prob = _KEEP_PROB,
             delta = _DELTA,
             sess_conf = None):

        ts_batch_gen = self.get_batch(x_ts, y_ts, max_iteration=0)

        if sess_conf is None:
            sess_conf = self.init_sess_config()

        with tf.Session(config=sess_conf) as sess:
            
            sess.run(tf.global_variables_initializer())
            print('Testing start ....')

            for epoch_i in range(epochs):
                loss_ts = 0
                num_batches = x_ts.shape[0]//self.batch_size

                for b in range(num_batches):
                    start = time.time()
                    x_batch, y_batch = next(ts_batch_gen)
                    # seq_len = np.array([list(x).index(0) + 1 for x in x_batch])
                    seq_len = np.array([len(list(x)) for x in x_batch])
                    loss, acc = sess.run(
                        [self.loss, self.accuracy],
                        feed_dict={
                            self.inputs: x_batch,
                            self.target: y_batch,
                            self.seq_len: seq_len,
                            self.keep_prob: keep_prob
                        }
                    )
                    loss_ts = loss * delta + loss_ts * (1 - delta)
                    period = time.time() - start
                    start += period 
                    print('Testing: Epoch {} Batch {}/{} - Period: {}'
                        .format(epoch_i, b, num_batches, period))
                    print('Loss: {} - Accuracy: {} '.format(loss_ts, acc))
            print('Testing end ....')
