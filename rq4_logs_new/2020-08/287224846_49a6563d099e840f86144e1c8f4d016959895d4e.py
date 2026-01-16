import gym
import numpy as np
from numpy import sin, cos, pi

o1 = 0.01
o2 = 0.05
o3 = 0.1
o4 = 0.3
o5 = 0.5
o6 = 0.7
o7 = 0.9
o8 = 0.95
o9 = 0.99

observation_list = {}
for t_1  in range(1, 11):
    observation_list[t_1] = {}
    for t_2 in range(1, 11):
        observation_list[t_1][t_2] = (t_1 - 1)*10 + t_2
observation_size = 100

legal_action = False
action_size = 3

h0 = 0.5
max_step = 10000

class Env():
    def __init__(self):
        self.env = gym.make('Acrobot-v1')
        self.reset()

    def discret(self, theta):
        p = (theta + pi) / (2*pi)
        if p <= o1:
            return 1
        elif p <= o2:
            return 2
        elif p <= o3:
            return 3
        elif p <= o4:
            return 4
        elif p <= o5:
            return 5
        elif p <= o6:
            return 6
        elif p <= o7:
            return 7
        elif p <= o8:
            return 8
        elif p <= o9:
            return 9
        else:
            return 10

    def get_observation(self):
        theta1 = self.env.env.state[0]
        theta2 = self.env.env.state[1]
        t_1 = self.discret(theta1)
        t_2 = self.discret(theta2)
        return observation_list[t_1][t_2]

    def reset(self):
        self.env.reset()
        self.env.env.state = np.array([0.0, 0.0, 0.0, 0.0])
        self.steps = 0
        return self.get_observation()

    def step(self, action):
        self.steps += 1
        self.env.step(action)
        if 2.0-cos(self.env.env.state[0])-cos(self.env.env.state[0]+self.env.env.state[1]) > h0:
            reward = 100.0
            done = True
        else:
            reward = 0.0
            done = False
        return self.get_observation(), reward, done