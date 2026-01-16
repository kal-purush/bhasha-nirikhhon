import os

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

from keras.models import Sequential, Model
from keras.layers import Dense, Input
from keras.optimizers import Adam
from keras.backend import set_session
import keras.backend as K
import tensorflow as tf
import numpy as np
import random


sess = tf.compat.v1.Session()
graph = tf.compat.v1.get_default_graph()
tf.compat.v1.disable_eager_execution()


HIDDEN_NUMBER = 64
LEARNING_RATE = 1e-3
BATCH_SIZE = 20
GAMMA = 0.975
EXPLORATION_DECAY = 0
EXPLORATION_MIN = 0
FILENAME_CRITIC = 'mazet_critic.h5'
FILENAME_ACTOR = 'mazet_actor.h5'

class Network:
    def __init__(self, observation_space, action_space):
        self.action_space = action_space
        self.exploration_decay = EXPLORATION_DECAY
        self.exploration_rate = 1

        self.state_memory = []
        self.action_memory = []
        self.reward_memory = []


        self.critic = Sequential()
        self.critic.add(Dense(HIDDEN_NUMBER, input_shape=(observation_space,), activation='tanh'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(HIDDEN_NUMBER, activation='relu'))
        self.critic.add(Dense(1, activation='linear'))
        self.critic.compile(loss='mse', optimizer=Adam(lr=1e-3))

        advantages = Input(shape=[1])

        def custom_loss_function(y_true, y_pred):
            out = K.clip(y_pred, 1e-8, 1 - 1e-8)
            log_lik = y_true * K.log(out)
            return K.sum(-log_lik * advantages)

        self.actor = self.create_actor(observation_space, advantages)
        self.actor.compile(loss=custom_loss_function, optimizer=Adam(lr=LEARNING_RATE))
        
        self.predictor = self.create_predictor(observation_space)

    def create_actor(self, observation_space, advantages):
        input = Input(shape=(observation_space,))
        dense1 = Dense(HIDDEN_NUMBER, activation='relu')(input)
        dense2 = Dense(HIDDEN_NUMBER, activation='relu')(dense1)
        dense3 = Dense(HIDDEN_NUMBER, activation='relu')(dense2)
        dense4 = Dense(HIDDEN_NUMBER, activation='relu')(dense3)
        dense5 = Dense(HIDDEN_NUMBER, activation='relu')(dense4)
        dense6 = Dense(HIDDEN_NUMBER, activation='relu')(dense5)
        dense7 = Dense(HIDDEN_NUMBER, activation='relu')(dense6)
        dense8 = Dense(HIDDEN_NUMBER, activation='relu')(dense7)
        dense9 = Dense(HIDDEN_NUMBER, activation='relu')(dense8)
        dense10 = Dense(HIDDEN_NUMBER, activation='relu')(dense9)
        dense11 = Dense(HIDDEN_NUMBER, activation='relu')(dense10)
        dense12 = Dense(HIDDEN_NUMBER, activation='relu')(dense11)
        dense13 = Dense(HIDDEN_NUMBER, activation='relu')(dense12)
        dense14 = Dense(HIDDEN_NUMBER, activation='relu')(dense13)
        dense15 = Dense(HIDDEN_NUMBER, activation='relu')(dense14)
        dense16 = Dense(HIDDEN_NUMBER, activation='relu')(dense15)
        dense17 = Dense(HIDDEN_NUMBER, activation='relu')(dense16)
        dense18 = Dense(HIDDEN_NUMBER, activation='relu')(dense17)
        dense19 = Dense(HIDDEN_NUMBER, activation='relu')(dense18)
        dense20 = Dense(HIDDEN_NUMBER, activation='relu')(dense19)
        dense21 = Dense(HIDDEN_NUMBER, activation='relu')(dense20)
        dense22 = Dense(HIDDEN_NUMBER, activation='relu')(dense21)
        dense23 = Dense(HIDDEN_NUMBER, activation='relu')(dense22)
        dense24 = Dense(HIDDEN_NUMBER, activation='relu')(dense23)
        dense25 = Dense(HIDDEN_NUMBER, activation='relu')(dense24)
        dense26 = Dense(HIDDEN_NUMBER, activation='relu')(dense25)
        dense27 = Dense(HIDDEN_NUMBER, activation='relu')(dense26)
        dense28 = Dense(HIDDEN_NUMBER, activation='relu')(dense27)
        dense29 = Dense(HIDDEN_NUMBER, activation='relu')(dense28)
        dense30 = Dense(HIDDEN_NUMBER, activation='relu')(dense29)
        dense31 = Dense(HIDDEN_NUMBER, activation='relu')(dense30)
        dense32 = Dense(HIDDEN_NUMBER, activation='relu')(dense31)
        probs = Dense(self.action_space, activation='softmax')(dense4)
        return Model(inputs = [input, advantages], outputs = [probs])

    def create_predictor(self, observation_space):
        input = Input(shape=(observation_space,))
        dense1 = Dense(HIDDEN_NUMBER, activation='relu')(input)
        dense2 = Dense(HIDDEN_NUMBER, activation='relu')(dense1)
        dense3 = Dense(HIDDEN_NUMBER, activation='relu')(dense2)
        dense4 = Dense(HIDDEN_NUMBER, activation='relu')(dense3)
        dense5 = Dense(HIDDEN_NUMBER, activation='relu')(dense4)
        dense6 = Dense(HIDDEN_NUMBER, activation='relu')(dense5)
        dense7 = Dense(HIDDEN_NUMBER, activation='relu')(dense6)
        dense8 = Dense(HIDDEN_NUMBER, activation='relu')(dense7)
        dense9 = Dense(HIDDEN_NUMBER, activation='relu')(dense8)
        dense10 = Dense(HIDDEN_NUMBER, activation='relu')(dense9)
        dense11 = Dense(HIDDEN_NUMBER, activation='relu')(dense10)
        dense12 = Dense(HIDDEN_NUMBER, activation='relu')(dense11)
        dense13 = Dense(HIDDEN_NUMBER, activation='relu')(dense12)
        dense14 = Dense(HIDDEN_NUMBER, activation='relu')(dense13)
        dense15 = Dense(HIDDEN_NUMBER, activation='relu')(dense14)
        dense16 = Dense(HIDDEN_NUMBER, activation='relu')(dense15)
        dense17 = Dense(HIDDEN_NUMBER, activation='relu')(dense16)
        dense18 = Dense(HIDDEN_NUMBER, activation='relu')(dense17)
        dense19 = Dense(HIDDEN_NUMBER, activation='relu')(dense18)
        dense20 = Dense(HIDDEN_NUMBER, activation='relu')(dense19)
        dense21 = Dense(HIDDEN_NUMBER, activation='relu')(dense20)
        dense22 = Dense(HIDDEN_NUMBER, activation='relu')(dense21)
        dense23 = Dense(HIDDEN_NUMBER, activation='relu')(dense22)
        dense24 = Dense(HIDDEN_NUMBER, activation='relu')(dense23)
        dense25 = Dense(HIDDEN_NUMBER, activation='relu')(dense24)
        dense26 = Dense(HIDDEN_NUMBER, activation='relu')(dense25)
        dense27 = Dense(HIDDEN_NUMBER, activation='relu')(dense26)
        dense28 = Dense(HIDDEN_NUMBER, activation='relu')(dense27)
        dense29 = Dense(HIDDEN_NUMBER, activation='relu')(dense28)
        dense30 = Dense(HIDDEN_NUMBER, activation='relu')(dense29)
        dense31 = Dense(HIDDEN_NUMBER, activation='relu')(dense30)
        dense32 = Dense(HIDDEN_NUMBER, activation='relu')(dense31)
        probs = Dense(self.action_space, activation='softmax')(dense4)
        return Model(inputs = [input], outputs = [probs])

    def add_to_memory(self, state, action, reward):
        self.state_memory.append(state[0])
        self.action_memory.append(action)
        self.reward_memory.append(reward)

    def take_action(self, state):
        if np.random.rand() < self.exploration_rate:
            return random.choice(range(self.action_space + 1))
        with graph.as_default():
            set_session(sess)
            policy = self.predictor.predict(state)
            return np.random.choice(range(self.action_space), p = policy[0])

    def experience_replay(self, iteration, epi):
        states = np.array(self.state_memory)
        actions = self.action_memory
        reward = self.reward_memory

        max_rew = np.amax(reward)
        min_rew = np.amin(reward)
        reward = (reward - min_rew) / (max_rew - min_rew)

        action_memory = np.zeros([len(actions), self.action_space])
        action_memory[np.arange(len(actions)), np.array(actions)] = 1

        with graph.as_default():
            set_session(sess)
            Max_Rewards = np.zeros_like(reward)
            BaseLine = np.zeros_like(reward)
            for t in range(len(reward)):
                max_reward = 0
                for k in range(t, len(reward)):
                    max_reward += reward[k]
                Max_Rewards[t] = max_reward
                BaseLine[t] = self.critic.predict(states[t].reshape([1, len(states[t])]))[0]

            # max_rew = np.amax(Max_Rewards)
            # min_rew = np.amin(Max_Rewards)
            # Max_Rewards = (Max_Rewards - min_rew) / (max_rew - min_rew)
            Advantage = Max_Rewards - BaseLine

            states = np.array(states)
            self.critic.fit(states, Max_Rewards, verbose=0, epochs=150, batch_size=5)
            self.actor.fit([states, Advantage], action_memory, verbose=0, epochs= 150, batch_size=5)

        self.exploration_rate *= self.exploration_decay
        self.exploration_rate = max(EXPLORATION_MIN, self.exploration_rate)


        self.state_memory = []
        self.action_memory = []
        self.reward_memory = []

        print("Iteration", iteration, "of", epi, "with exploration of", self.exploration_rate)

    def update_model(self):
        with graph.as_default():
            set_session(sess)
            self.predictor.set_weights(self.actor.get_weights())

    def save_model(self):
        with graph.as_default():
            set_session(sess)
            self.critic.save_weights(FILENAME_CRITIC)
            self.actor.save_weights(FILENAME_ACTOR)

    def load_model(self):
        if  os.path.exists(FILENAME_ACTOR):
            with graph.as_default():
                set_session(sess)
                self.actor.load_weights(FILENAME_ACTOR)
                self.predictor.set_weights(self.actor.get_weights())
        if os.path.exists(FILENAME_CRITIC):
            with graph.as_default():
                set_session(sess)
                self.critic.load_weights(FILENAME_CRITIC)
        else:
            print("Model doesn't exist, ignoring")