import tensorflow as tf
from sklearn.metrics import roc_auc_score, log_loss
from time import time
import numpy as np

class DFM:
    def __init__(self, feature_size, field_size, lr_l2_reg=0.1, 
               dm_l2_reg=0.1, epoch=10, learning_rate=0.01, 
               batch_size=1024, optimizer_type="adam", greater_is_better=True):
        self.sess = None
        self.loss = None
        self.optimizer = None

        self.feature_size = feature_size
        self.field_size = field_size
        self.lr_l2_reg = lr_l2_reg #logistic regression l2 norm
        self.dm_l2_reg = dm_l2_reg #delay model l2 nore

        self.epoch = epoch
        self.learning_rate = learning_rate
        self.optimizer_type = optimizer_type
        self.batch_size = batch_size

        self.greater_is_better = greater_is_better

        self.train_result, self.valid_result = [], []
        self._init_graph()

    def _init_graph(self):
        self.graph = tf.Graph()
        with self.graph.as_default():
            tf.set_random_seed(2019)
            self.fea_index = tf.placeholder(tf.int32, shape=[None, None], name='fea_index')
            self.fea_value = tf.placeholder(tf.float32, shape=[None, None], name='fea_value')

            self.label = tf.placeholder(tf.float32, shape=[None, 1], name='label')
            self.delay_time = tf.placeholder(tf.float32, shape=[None, 1], name='delay_time')

            #logitic regression paramter
            self.wc = tf.Variable(
                tf.random_normal([self.feature_size, 1], stddev=0.01), name='wc')

            #delay model paramter
            self.wd = tf.Variable(
                tf.random_normal([self.feature_size, 1], stddev=0.01), name='wd')
            
            fea_value = tf.reshape(self.fea_value, shape=[-1, self.field_size, 1])
            self.wc_x = tf.reduce_sum(
                tf.multiply(
                    tf.nn.embedding_lookup(self.wc, self.fea_index), 
                    fea_value
                ),
                axis=1
            )

            self.wd_x = tf.reduce_sum(
                tf.multiply(
                    tf.nn.embedding_lookup(self.wd, self.fea_index), 
                    fea_value
                    ),
                axis=1
            )

            self.p_x = tf.nn.sigmoid(self.wc_x)
            self.lamb_x = tf.exp(self.wd_x)

            self.nll = -self.label*(tf.log(self.p_x)+tf.log(self.lamb_x)-self.delay_time*self.lamb_x) \
                -(1-self.label)*(tf.log(1-self.p_x+self.p_x*tf.exp(-self.lamb_x*self.delay_time)))

            self.loss = tf.reduce_mean(
                self.nll,
                axis=1
            )

            self.loss += tf.contrib.layers.l2_regularizer(self.lr_l2_reg)(self.wc)
            self.loss += tf.contrib.layers.l2_regularizer(self.dm_l2_reg)(self.wd)


            # optimizer
            if self.optimizer_type == "adam":
                self.optimizer = tf.train.AdamOptimizer(learning_rate=self.learning_rate, beta1=0.9, beta2=0.999,
                                                        epsilon=1e-8).minimize(self.loss)
            elif self.optimizer_type == "adagrad":
                self.optimizer = tf.train.AdagradOptimizer(learning_rate=self.learning_rate,
                                                           initial_accumulator_value=1e-8).minimize(self.loss)
            elif self.optimizer_type == "gd":
                self.optimizer = tf.train.GradientDescentOptimizer(learning_rate=self.learning_rate).minimize(self.loss)
            elif self.optimizer_type == "momentum":
                self.optimizer = tf.train.MomentumOptimizer(learning_rate=self.learning_rate, momentum=0.95).minimize(
                    self.loss)

            init = tf.global_variables_initializer()
            self.sess = self._init_session()
            self.sess.run(init)


    def _init_session(self):
        config = tf.ConfigProto(device_count={"gpu": 0})
        config.gpu_options.allow_growth = True
        return tf.Session(config=config)


    def get_batch(self, Xi, Xv, y, y_dt, batch_size, index):
        start = index * batch_size
        end = (index+1) * batch_size
        end = end if end < len(y) else len(y)
        if y_dt is not None:
            return Xi[start:end], Xv[start:end], [[y_] for y_ in y[start:end]], [[y_dt_] for y_dt_ in y_dt[start:end]]
        else:
            return Xi[start:end], Xv[start:end], [[y_] for y_ in y[start:end]]

    # shuffle three lists simutaneously
    def shuffle_in_unison_scary(self, a, b, c, d):
        rng_state = np.random.get_state()
        np.random.shuffle(a)
        np.random.set_state(rng_state)
        np.random.shuffle(b)
        np.random.set_state(rng_state)
        np.random.shuffle(c)
        np.random.set_state(rng_state)
        np.random.shuffle(d)

    def fit_on_batch(self, Xi, Xv, y, dt):
        feed_dict = {self.fea_index: Xi,
                     self.fea_value: Xv,
                     self.label: y,
                     self.delay_time: dt}
        loss, opt, p, l, nill = self.sess.run((self.loss, self.optimizer, self.p_x, self.lamb_x, self.nll), feed_dict=feed_dict)
        #print "label=%s\n, dt=%s\n, p_x=%s\n, lamb_x=%s\n, nill=%s\n" % (y, dt, p, l, nill)   
        return loss

    def fit(self, Xi_train, Xv_train, y_train, y_dt,
            Xi_valid=None, Xv_valid=None, y_valid=None,
            early_stopping=False, refit=False):
        has_valid = Xv_valid is not None
        for epoch in range(self.epoch):
            t1 = time()
            self.shuffle_in_unison_scary(Xi_train, Xv_train, y_train, y_dt)
            total_batch = int(len(y_train) / self.batch_size)
            for i in range(total_batch):
                Xi_batch, Xv_batch, y_batch, y_dt_batch = self.get_batch(Xi_train, Xv_train, y_train, y_dt, self.batch_size, i)
                #print "train loss: %s " % self.fit_on_batch(Xi_batch, Xv_batch, y_batch, y_dt_batch)

            # evaluate training and validation datasets
            train_result = self.evaluate(Xi_train, Xv_train, y_train)
            self.train_result.append(train_result)
            if has_valid:
                valid_result = self.evaluate(Xi_valid, Xv_valid, y_valid)
                self.valid_result.append(valid_result)
            self.verbose = 1
            if self.verbose > 0 and epoch % self.verbose == 0:
                if has_valid:
                    print("[%d] train-result=%.4f, valid-result=%.4f [%.1f s]"
                        % (epoch + 1, train_result, valid_result, time() - t1))
                else:
                    print("[%d] train-result=%.4f [%.1f s]"
                        % (epoch + 1, train_result, time() - t1))
            if has_valid and early_stopping and self.training_termination(self.valid_result):
                break


    def training_termination(self, valid_result):
        if len(valid_result) > 5:
            if self.greater_is_better:
                if valid_result[-1] < valid_result[-2] and \
                    valid_result[-2] < valid_result[-3] and \
                    valid_result[-3] < valid_result[-4] and \
                    valid_result[-4] < valid_result[-5]:
                    return True
            else:
                if valid_result[-1] > valid_result[-2] and \
                    valid_result[-2] > valid_result[-3] and \
                    valid_result[-3] > valid_result[-4] and \
                    valid_result[-4] > valid_result[-5]:
                    return True
        return False
        
    def evaluate(self, Xi, Xv, y):
        """
        :param Xi: list of list of feature indices of each sample in the dataset
        :param Xv: list of list of feature values of each sample in the dataset
        :param y: label of each sample in the dataset
        :return: metric of the evaluation
        """
        y_pred = self.predict(Xi, Xv)
        return roc_auc_score(y, y_pred)

    def predict(self, Xi, Xv):
        dummy_y = [1] * len(Xi)
        batch_index = 0
        Xi_batch, Xv_batch, y_batch = self.get_batch(Xi, Xv, dummy_y, None, self.batch_size, batch_index)
        y_pred = None
        while len(Xi_batch) > 0:
            num_batch = len(y_batch)
            feed_dict = {self.fea_index: Xi_batch,
                         self.fea_value: Xv_batch,
                         self.label: y_batch}
            batch_out = self.sess.run(self.p_x, feed_dict=feed_dict)
            if batch_index == 0:
                y_pred = np.reshape(batch_out, (num_batch,))
            else:
                y_pred = np.concatenate((y_pred, np.reshape(batch_out, (num_batch,))))

            batch_index += 1
            Xi_batch, Xv_batch, y_batch = self.get_batch(Xi, Xv, dummy_y, None, self.batch_size, batch_index)

        return y_pred