import pandas as pd
import os
import tensorflow as tf
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from imblearn.over_sampling import SMOTE
import numpy as np

def train_model(data_file='data.csv', model_file='model.h5', epochs=10, batch_size=32, test_size=0.2, random_state=42):
    smote = SMOTE(random_state=random_state)
    data = pd.read_csv(data_file, sep=';', header=0)

    print('Total rows:', len(data))
    print('Rows with medal:', len(data.dropna(subset=['Medal'])))

    data = pd.get_dummies(data, columns=['Sex', 'Medal'])
    data = data.drop(columns=['Name', 'Team', 'NOC', 'Games', 'Year', 'Season', 'City', 'Sport', 'Event'])

    scaler = MinMaxScaler()
    data = pd.DataFrame(scaler.fit_transform(data), columns=data.columns)

    X = data.filter(regex='Sex|Age')
    y = data.filter(regex='Medal')
    y = pd.get_dummies(y)

    X = X.fillna(0)
    y = y.fillna(0)

    y = y.values

    X_resampled, y_resampled = smote.fit_resample(X, y)
    X_train, X_test, y_train, y_test = train_test_split(X_resampled, y_resampled, test_size=test_size, random_state=random_state)

    model = tf.keras.models.Sequential()
    model.add(tf.keras.layers.Dense(64, input_dim=X_train.shape[1], activation='relu')) 
    model.add(tf.keras.layers.Dense(32, activation='relu'))
    model.add(tf.keras.layers.Dense(y.shape[1], activation='softmax')) 

    model.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])

    model.fit(X_train, y_train, epochs=epochs, batch_size=batch_size)
    loss, accuracy = model.evaluate(X_test, y_test)
    print('Test accuracy:', accuracy)
    print('Test loss:', loss)

    model.save(model_file)

    return accuracy

def run_experiment():
    accuracy = train_model()
    print('Model accuracy:', accuracy)

run_experiment()