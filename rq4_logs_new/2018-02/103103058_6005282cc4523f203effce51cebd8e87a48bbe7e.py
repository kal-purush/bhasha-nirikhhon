import os

import numpy as np
from keras import optimizers
from keras.layers import Activation, Dense, Dropout
from keras.layers.recurrent import LSTM
from keras.models import Sequential
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn.svm import SVR

from src.process_data import process_naive

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

def train_model(symbol, db):

    X, y = process_naive(symbol, db)
    print(X.shape, y.shape)
    scaler = MinMaxScaler()
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, train_size=0.80, shuffle=False)

    train = np.append(X_train, y_train[:, None], axis=1)
    test = np.append(X_test, y_test[:, None], axis=1)
    #print(test)
    y_index = train.shape[1] - 1

    scaler.fit(train)

    train = scaler.transform(train)
    test = scaler.transform(test)

    X_train = train[:, 0:y_index]
    X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
    print(X_train.shape)
    y_train = train[:, y_index]

    X_test = test[:, 0:y_index]

    y_test = test[:, y_index]

    model = Sequential()
    model.add(LSTM(64, input_shape=(6,1), stateful=False))
    model.add(Dropout(0.2))
    model.add(Dense(1, activation='sigmoid'))
    #model.add(Dense(output_dim=1, input_dim=6, kernel_initializer = 'uniform', activation = 'relu'))
    sgd = optimizers.SGD(lr=0.3, momentum=.2, decay=.1)
    model.compile(loss='mean_squared_error', optimizer=sgd)

    model.fit(x=X_train, y=y_train, epochs=200, batch_size=2, verbose=1, validation_split=.1)

    #y_pred = model.evaluate(x=X_test, y=y_test, batch_size=2)

    print(y_pred)


class NaiveSeq:

    def __init__(self, db, **kwargs):

        self.__model = Sequential(**kwargs)
        self.__model.add(Dense(8, input_dim=6, activation='relu'))
        self.__model.add(Dense(1))

        self.__model.compile(loss='mean_squared_error', optimizer='adam', metrics=['accuracy'])
        self.__db = db
        
    def train_model(self, symbol, epochs=100, batch_size=2, verbose=2, **kwargs):
    
        X, y = process_naive(symbol, self.__db)

        X_train, self.__X_test, y_train, self.y_test = train_test_split(X, y, test_size=0.15, train_size=0.85)
        self.__model.fit(X_train, y_train, batch_size, epochs, verbose)


class NaiveSVR:

    def __init__(self, db, **kwargs):

        self.__clf = SVR(
            C=.85,
            epsilon=0.1,
            kernel='poly',
            degree=13,
            gamma='auto',
            coef0=0.0,
            shrinking=True,
            verbose=True
        )
        self.__db = db

    def train_model(self, symbol):

        X, y = process_naive(symbol, self.__db, 3, 2)
        X_train, self.__X_test, y_train, self.__y_test = train_test_split(X, y, test_size=0.15, train_size=0.85)

        self.__clf.fit(X_train, y_train)

    def evaluate_model(self):
        pred_y = self.__clf.predict(self.__X_test)
        #for i in zip(pred_y, self.__y_test):

            #print(i)