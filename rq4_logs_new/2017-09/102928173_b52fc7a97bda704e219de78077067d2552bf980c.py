#######################################################################
# Copyright (C)                                                       #
# 2016 Shangtong Zhang(zhangshangtong.cpp@gmail.com)                  #
# 2016 Kenta Shimada(hyperkentakun@gmail.com)                         #
# Permission given to modify the code as long as you keep this        #
# declaration at the top                                              #
#######################################################################

import numpy as np
import matplotlib.pyplot as plt


class Gamble(object):
    """Gamble Model"""

    def __init__(self, goal, head_probability):
        self.goal = goal  # goal of gambling
        self.states = np.arange(goal + 1)  # all states with terminals
        self.head = head_probability  # probability for head up
        self.value_state = np.zeros(goal + 1)
        self.value_state[goal] = 1  # initalize state values
        self.discount = 1

    # def decide(self, state, action):
    #     """Define task"""
    #     next_states = [state + action, state - action]
    #     rewards = [self.discount * self.value_state[next_state] for next_state in next_states]
    #     probabilities = [self.head, 1 - self.head]
    # return np.array(next_states), np.array(rewards), np.array(probabilities)

    def improve(self, eps=1e-9):
        """improve policy with value iteration"""
        sweep = 0
        while True:
            new_value = np.zeros(self.goal + 1)
            for state in self.states:
                actions = np.arange(min(state, self.goal - state) + 1)
                new_value[state] = np.max([self.head * self.value_state[state + action] + (
                    1 - self.head) * self.value_state[state - action] for action in actions])
            if np.sum(np.abs(new_value - self.value_state)) < eps:
                self.value_state = new_value
                break
            self.value_state = new_value
            sweep += 1
            plt.plot(self.value_state, label=sweep)
            if sweep in [1, 2, 3, 20]:
                plt.annotate('sweep %d' % sweep, xy=(47, self.value_state[47]), xytext=(57, self.value_state[47] + 0.05), arrowprops=dict(facecolor='black', headwidth=5, width=0.5))

        # find optimal policy
        policy = np.zeros(self.goal + 1)
        for state in self.states[1:self.goal]:
            # exclude 0 stake so no 0 & goal state
            actions = np.arange(1, min(state, self.goal - state) + 1)
            policy[state] = actions[np.argmax([self.head * self.value_state[state + action] + (
                1 - self.head) * self.value_state[state - action] for action in actions])]

        return self.value_state, policy


plt.figure(0)
gambler = Gamble(100, 0.25)
value_state, optimal_policy = gambler.improve(eps=1e-10)
plt.xlabel('Capital')
plt.ylabel('Value State')
plt.figure(1)
plt.scatter(gambler.states, optimal_policy)
plt.xlabel('Capital')
plt.ylabel('Optimal Policy')
plt.show()