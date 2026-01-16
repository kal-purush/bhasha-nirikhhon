"""
==========
Keras test
==========

"""

import convert_data

import numpy as np

from keras.models import Sequential
from keras.layers import Dense, SimpleRNN, Dropout, Activation
from keras.regularizers import l1, l2, l1l2, activity_l1, activity_l2, \
    activity_l1l2
from keras.optimizers import SGD
from keras.callbacks import EarlyStopping, LearningRateScheduler, TensorBoard
from keras.utils.np_utils import to_categorical
# from keras.utils.visualize_util import plot

import time

def main():
    ## Loading the data
    D, params = convert_data.load_data(pca=0)

    X_train, y_train = D[0]
    X_test, y_test = D[1]

    n_features, n_classes = params[0], params[1]
    n_samples_train, n_samples_test = params[2]

    ## Building the model
    print('... building the model')

    model = Sequential()
    activation = 'relu'
    # recurrent_layer_size = 50
    # hidden_layer_size = 50
    init = 'uniform'
    regularization = l2(0.1)  #l1l2(0.0001)
    activity_regularization = activity_l2(0.05)
    dropout = 0.5

    # Input layer

    # Recurrent layer
    # model.add(SimpleRNN(hidden_layer_size, init='glorot_uniform',
    #                     inner_init='orthogonal', activation='tanh',
    #                     W_regularizer=None, U_regularizer=None,
    #                     b_regularizer=None, dropout_W=0.0, dropout_U=0.0)
    #           )

    # Hidden layers
    model.add(Dense(120, input_dim=n_features, init=init,
                    activation=activation, W_regularizer=regularization,
                    activity_regularizer=activity_regularization))
    model.add(Dropout(dropout))
    model.add(Dense(30, init=init, activation=activation,
                    W_regularizer=regularization,
                    activity_regularizer=activity_regularization))
    model.add(Dropout(dropout))
    model.add(Dense(30, init=init, activation=activation,
                    W_regularizer=regularization,
                    activity_regularizer=activity_regularization))
    model.add(Dropout(dropout))

    # Output layer
    model.add(Dense(n_classes, init=init, activation='softmax'))

    ## Compiling the model
    sgd = SGD(lr=0.003, decay=0.00, momentum=0.5, nesterov=False)
    model.compile(loss='categorical_crossentropy',
                  optimizer=sgd,
                  metrics=['accuracy']
                 )
    # plot(model, to_file='../logs/model.png')

    ## Training
    # Callbacks
    # early_stopping = EarlyStopping(monitor='val_loss', patience=5000,
    #                                verbose=1, mode='auto')
    # lr_scheduler = LearningRateScheduler()
    y_train = to_categorical(y_train, n_classes)

    print('... training')
    start_time = time.time()

    model.fit(X_train, y_train,
              batch_size=15,
              nb_epoch=300000,
              verbose=1,
              callbacks=[],
              validation_split=0.15,
              validation_data=None,
              shuffle=True,
              class_weight=None,
              sample_weight=None
             )

    time = time.time() - start_time
    print('... training took {}min {}sec'.format(int(time/60), int(time%60)))

    ## Prediction
    # y_test = to_categorical(y_test, n_classes)
    # score = model.evaluate(X_test, y_test,
    #                        batch_size=1,
    #                        verbose=1,
    #                        sample_weight=None
    #                       )
    # print()
    # print('loss: {} - acc: {}'.format(*score))

if __name__ == '__main__':
    main()