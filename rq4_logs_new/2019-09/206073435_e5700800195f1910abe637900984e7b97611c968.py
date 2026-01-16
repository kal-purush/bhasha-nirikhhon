import random
import numpy as np
import pandas as pd
from keras.optimizers import Adam
from keras.models import Sequential
from keras.layers.core import Dense, Dropout
from operator import add
from src.Config import Config

class DQNAgent:

    def __init__(self):
        self.reward = 0
        self.gamma = 0.9
        self.df = pd.DataFrame()
        self.short_memory = np.array([])
        self.agent_target = 1
        self.agent_predict = 0
        self.learning_rate = 0.001
        self.model = self.network()
        self.actual = []
        self.memory = []

    def get_state(self, snake, apple):
        change = Config['snake']['speed']
        bumper = Config['game']['bumper_size']
        bumper_x = Config['game']['width'] - bumper
        bumper_y = Config['game']['height'] - bumper
        snake_width = Config['snake']['width']
        snake_height = Config['snake']['height']
        # 11 eleven different states
        state = [
        # - immediate danger straight : check if we're heading to a wall or our tail
        (snake.x_change == change and (((snake.x_pos + change, snake.y_pos) in snake.body) or (snake.x_pos + snake_width > bumper_x - 2*change))) or
        (snake.x_change == -change and (((snake.x_pos - change, snake.y_pos) in snake.body) or (snake.x_pos < bumper + 2*change))) or
        (snake.y_change == change and (((snake.x_pos, snake.y_pos + change) in snake.body) or (snake.y_pos + snake_height > bumper_y - 2*change))) or
        (snake.y_change == -change and (((snake.x_pos, snake.y_pos - change) in snake.body) or (snake.y_pos < bumper + 2*change))),
        # - immediate danger on the right
        (snake.y_change == -change and (((snake.x_pos + change, snake.y_pos) in snake.body) or (snake.x_pos + snake_width > bumper_x - 2*change))) or
        (snake.y_change == change and (((snake.x_pos - change, snake.y_pos) in snake.body) or (snake.x_pos < bumper + 2*change))) or
        (snake.x_change == change and (((snake.x_pos, snake.y_pos + change) in snake.body) or (snake.y_pos + snake_height > bumper_y - 2*change))) or
        (snake.x_change == -change and (((snake.x_pos, snake.y_pos - change) in snake.body) or (snake.y_pos < bumper + 2*change))),
        # - immediate danger on the left
        (snake.y_change == change and (((snake.x_pos + change, snake.y_pos) in snake.body) or (snake.x_pos + snake_width > bumper_x - 2*change))) or
        (snake.y_change == -change and (((snake.x_pos - change, snake.y_pos) in snake.body) or (snake.x_pos < bumper + 2*change))) or
        (snake.x_change == -change and (((snake.x_pos, snake.y_pos + change) in snake.body) or (snake.y_pos + snake_height > bumper_y - 2*change))) or
        (snake.x_change == change and (((snake.x_pos, snake.y_pos - change) in snake.body) or (snake.y_pos < bumper + 2*change))),

        #  - move left
        snake.x_change == -change,
        #  - move right
        snake.x_change == change,
        #  - move up
        snake.y_change == -change,
        #  - move down
        snake.y_change == change,

        # - apple left
        apple.x_pos < snake.x_pos,
        # - apple right
        apple.x_pos > snake.x_pos,
        # - apple up
        apple.y_pos < snake.y_pos,
        # - apple down
        apple.y_pos > snake.y_pos
        ]
        state = [int(e) for e in state]

        return np.array(state)

    def network(self):
        model = Sequential()
        # Input layer
        model.add(Dense(output_dim=100, activation='relu', input_dim=11))
        model.add(Dropout(0.15)) # Avoid overfitting
        # Hidden layers
        model.add(Dense(output_dim=100, activation='relu'))
        model.add(Dropout(0.15))
        model.add(Dense(output_dim=100, activation='relu'))
        model.add(Dropout(0.15))
        # Output layer
        model.add(Dense(output_dim=3, activation='softmax'))
        opt = Adam(self.learning_rate)
        model.compile(loss='mse', optimizer=opt)
        return model

    def remember(self, state, action, reward, next_state, done):
        self.memory.append((state, action, reward, next_state, done))

    def replay_new(self, memory):
        if len(memory) > 1000:
            minibatch = random.sample(memory, 1000)
        else:
            minibatch = memory
        for state, action, reward, next_state, done in minibatch:
            target = reward
            if not done:
                target = reward + self.gamma * np.amax(self.model.predict(np.array([next_state]))[0])
            target_f = self.model.predict(np.array([state]))
            target_f[0][np.argmax(action)] = target
            self.model.fit(np.array([state]), target_f, epochs=1, verbose=0)

    def train_short_memory(self, state, action, reward, next_state, done):
        target = reward
        if not done:
            target = reward + self.gamma * np.amax(self.model.predict(np.array([next_state]))[0])
        target_f = self.model.predict(state.reshape((1, 11)))
        target_f[0][np.argmax(action)] = target
        self.model.fit(state.reshape((1, 11)), target_f, epochs=1, verbose=0)

    def set_reward(self, snake, is_dead):
        self.reward = 0
        if is_dead:
            self.reward = -10
        if snake.eaten:
            self.reward = 15
        return self.reward