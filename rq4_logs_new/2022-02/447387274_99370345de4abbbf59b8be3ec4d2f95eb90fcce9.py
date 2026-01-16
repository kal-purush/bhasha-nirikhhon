from tensorflow.keras import layers
from tensorflow.keras.models import Sequential
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.callbacks import ModelCheckpoint
from tensorflow.keras import optimizers
import os
import json

input = json.load(open("input.json", "r"))


def build_model(window_length, num_features):
    # setup the model
    model = Sequential()
    model.add(
        layers.LSTM(units=128, return_sequences=True, input_shape=(window_length, num_features), activation='tanh'))
    # model.add(Dropout(0.1))
    model.add(layers.LSTM(units=128, activation='tanh'))
    # model.add(Dropout(0.1))
    model.add(layers.Dense(64, activation='tanh'))
    # model.add(Dropout(0.1))
    model.add(layers.Dense(16, activation='tanh'))
    # model.add(Dropout(0.1))
    model.add(layers.Dense(1, activation='tanh'))

    return model


def compile_and_fit(paths, model, patience, loss, metric, train_x, train_y, learning_rate=0.001):
    # callback setting
    callbacks = [
        EarlyStopping(monitor='val_loss', patience=patience, mode='min'),
        # ModelCheckpoint(filepath=paths["check_point"], monitor='val_loss', save_best_only=True, mode='min')
    ]

    # compile
    model.compile(loss=loss,
                  optimizer=optimizers.Adam(learning_rate=learning_rate),  # default learning_rate is 0.001
                  metrics=metric)

    # fit the model
    history = model.fit(train_x, train_y,
                        epochs=10, batch_size=32, verbose=2,
                        validation_split=0.20,
                        callbacks=callbacks
                        )

    savedir = paths["model"]
    os.makedirs(savedir, exist_ok=True)
    model.save(savedir)

    return history