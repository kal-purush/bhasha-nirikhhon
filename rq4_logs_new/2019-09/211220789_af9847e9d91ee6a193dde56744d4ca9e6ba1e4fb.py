import gym
import numpy as np
from flappy_gym.envs.flappy_env import FlappyEnv
from flappy_gym import envs

env = gym.make('flappygym-v0')
env.reset()


LEARNING_RATE = 0.99
DISCOUNT = 0.95
EPISODES = 2500
actions = 2


q_table = np.random.uniform(low=-2, high=0, size=(288, 512, 2))  # 285 is highest the pipe can be


for i in range(EPISODES):
    state = env.reset()
    done = False
    while not done:
        x, y = state
        x = int(x)
        y = int(y)
        action = np.argmax(q_table[x][y])
        action = int(action)
        new_state, reward, done, _ = env.step(action)
        if i % 100 == 0:
            env.render()

        # if simulation didn't end after the previous step, update q-table
        if not done:
            x_new, y_new = new_state
            max_future_q = np.max(q_table[int(x_new)][int(y_new)])

            current_q = q_table[x][y][action]   # current q-value

            # new q-value using the Q-formula
            new_q = (1 - LEARNING_RATE) * current_q + LEARNING_RATE * (reward + DISCOUNT * max_future_q)

            q_table[x][y][action] = new_q   # updates q-table with newly generated q-value

        # Simulation ended (for any reason) - if goal position is achieved - update Q value with reward directly
        # elif new_state[0] >= env.goal_position:
            # q_table[state + (action,)] = 0

        state = new_state

env.close()



