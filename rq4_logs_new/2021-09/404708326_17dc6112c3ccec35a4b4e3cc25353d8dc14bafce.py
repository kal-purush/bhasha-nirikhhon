import gym
import numpy as np
import random
from utils import argmax
from gym.envs.toy_text.frozen_lake import generate_random_map
import sys

random_map = generate_random_map(size=20, p=0.4)
env = gym.make("FrozenLake-v1", desc=random_map)
Q = np.zeros((env.observation_space.n, env.action_space.n))

episodes = 100000
alpha = 0.01
gamma = 0.8
epsilon = 0.4
living_reward = -0.0001

reward_list = []
for episode in range(episodes):
    state = env.reset()

    while True:

        if random.uniform(0,1)<=epsilon:
            action = env.action_space.sample()
        else:
            action = argmax(Q[state])

        next_state, reward, done, inf = env.step(action)
        reward += living_reward
        next_max = np.max(Q[next_state])
        q = (1-alpha)*Q[state, action]+alpha*(reward+gamma*next_max)

        Q[state, action] = q
        state = next_state

        if done:
            reward_list.append(reward)
            break
        
        if episode%100 == 0:
            # sys.stdout.flush()
            print(episode ,end="\r")
            

np.save("Q_table.npy", Q)



