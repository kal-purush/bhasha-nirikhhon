#######################################################################
# Copyright (C)                                                       #
# 2016 Shangtong Zhang(zhangshangtong.cpp@gmail.com)                  #
# 2016 Kenta Shimada(hyperkentakun@gmail.com)                         #
# Permission given to modify the code as long as you keep this        #
# declaration at the top                                              #
#######################################################################

import numpy as np


class Grid(object):
    """Model of environment"""

    def __init__(self, world_size, terminal, initial_state):
        self.size = world_size
        self.row = world_size + 1  # row of grid for a new state
        self.column = world_size  # coulume of the grid
        self.terminal = terminal  # sites for terminal states
        self.state = initial_state  # state at time t
        self.actions = ['U', 'D', 'L', 'R']  # actions allowed
        self.rewards = []  # rewards for state-action pair
        self.next_states = []  # next state for the task
        self.init_task()
        # value for each state
        self.value_states = np.zeros((self.row, self.column))

    def init_task(self):
        """dynamics of the environment and states"""
        for i in range(0, self.row):
            self.rewards.append([])
            self.next_states.append([])
            for j in range(0, self.column):
                next_state = dict()
                reward = dict()

                if i == 0:
                    next_state['U'] = [i, j]
                else:
                    next_state['U'] = [i - 1, j]
                reward['U'] = -1

                if i == self.size - 1:
                    next_state['D'] = [i, j]
                    if j == 1:
                        # new dynamics for state 13
                        next_state['D'] = [i + 1, j]
                else:
                    next_state['D'] = [i + 1, j]
                reward['D'] = -1

                if j == 0:
                    next_state['L'] = [i, j]
                else:
                    next_state['L'] = [i, j - 1]
                reward['L'] = -1

                if j == self.size - 1:
                    next_state['R'] = [i, j]
                else:
                    next_state['R'] = [i, j + 1]
                reward['R'] = -1

                # states in new row are terminal states only accessible to themselves
                if [i, j] in self.terminal + [[self.row - 1, _] for _ in range(0, self.column)]:
                    next_state['U'] = next_state['D'] = next_state['L'] = next_state['R'] = [i, j]
                    reward['U'] = reward['D'] = reward['L'] = reward['R'] = 0

                # dynamics for the new state
                if i == self.row - 1 and j == 1:
                    next_state['U'] = [self.row - 2, j]
                    next_state['D'] = [self.row - 1, j]
                    next_state['L'] = [self.row - 2, j - 1]
                    next_state['R'] = [self.row - 2, j + 1]
                    reward['U'] = reward['D'] = reward['L'] = reward['R'] = -1
                
                self.next_states[i].append(next_state)
                self.rewards[i].append(reward)

    def evaluate_policy(self, action_probability, discount=1, eps=1e-9):
        """evaluate policy"""
        while True:
            new_value = np.zeros((self.row, self.column))  # get a new location in the memory
            for i in range(0, self.row):
                for j in range(0, self.column):
                    # update with full backup
                    new_value[i][j] = np.sum([action_probability[i][j][action] * (self.rewards[i][j][action] + discount * self.value_states[self.next_states[i][j][action][0], self.next_states[i][j][action][1]]) for action in self.actions])
            if np.sum(np.abs(self.value_states - new_value)) < eps:
                break
            self.value_states = new_value  # give adress of variable since variable stores location
        return self.value_states

    def decide(self, action):
        """define environment"""
        state = self.state
        self.state = self.next_states[state[0]][state[1]][action]
        return self.rewards[state[0]][state[1]][action], self.state


WORLD_SIZE = 4
TERMINAL = [[0, 0], [WORLD_SIZE - 1, WORLD_SIZE - 1]]
grid = Grid(WORLD_SIZE, TERMINAL, [0, 0])
# random policy
action_probability = []
for i in range(0, WORLD_SIZE + 1):
    action_probability.append([])
    for j in range(0, WORLD_SIZE):
        action_probability[i].append(dict({'L': 0.25, 'U': 0.25, 'R': 0.25, 'D': 0.25}))
print(grid.evaluate_policy(action_probability))

# error: new_value needs to be redifined every time due to the way Python defines the variable