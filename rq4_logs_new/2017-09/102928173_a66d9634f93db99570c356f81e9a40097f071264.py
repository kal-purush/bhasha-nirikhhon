#######################################################################
# Copyright (C)                                                       #
# 2016 Shangtong Zhang(zhangshangtong.cpp@gmail.com)                  #
# 2016 Tian Jun(tianjun.cpp@gmail.com)                                #
# 2016 Artem Oboturov(oboturov@gmail.com)                             #
# 2016 Kenta Shimada(hyperkentakun@gmail.com)                         #
# Permission given to modify the code as long as you keep this        #
# declaration at the top                                              #
#######################################################################

import matplotlib.pyplot as plt
import numpy as np


class Action(object):
    """model for action with random walk"""

    def __init__(self, estimate=0, value=0, stepsize=0):
        self.count = 0  # the numbers of being taken
        self.estimate = estimate  # estimate of action value
        self.value = value  # value of the action
        self.stepsize = stepsize  # stepsize for the estimate, default sample average

    def take_action(self):
        """get reward, update estimate"""
        reward = np.random.randn() + self.value
        self.count += 1
        # update the estimate
        if self.stepsize == 0:
            self.estimate += (reward - self.estimate) / self.count
        else:
            self.estimate += (reward - self.estimate) * self.stepsize
        return reward


class Bandit(object):
    """model for k-armed bandit"""

    def __init__(self, k_arm=10, epsilon=0, stepsize=0, walk=0):
        self.k = k_arm  # number of arms
        self.epsilon = epsilon  # probability of exploration
        self.actions = [Action(stepsize=stepsize) for _ in range(0, self.k)]  # action list
        self.time = 0
        self.walk = walk  # the random walk length
        self.best_action = np.argmax([self.actions[i].value for i in range(0, self.k)])

    def take_step(self):
        """select action and take it in one step and value goes random walk"""
        self.time += 1
        # exploration or exploting
        if np.random.binomial(1, self.epsilon) == 1:
            action = np.random.randint(0, self.k)
        else:
            action = np.argmax([self.actions[i].estimate for i in range(0, self.k)])
        self.best_action = np.argmax([self.actions[i].value for i in range(0, self.k)])
        reward = self.actions[action].take_action()
        # random walk every 20 steps
        if time % 20 == 0:
            for i in range(0, self.k):
                if np.random.binomial(1, 0.5) == 1:
                    self.actions[i].value += self.walk
                else:
                    self.actions[i].value -= self.walk
        return action, reward


def banditSimulation(nBandits, time, bandits):
    """Play the bandits with nBandits runs each has time steps"""
    best_action_counts = [np.zeros(time, dtype=float) for _ in range(0, len(bandits))]
    average_rewards = [np.zeros(time, dtype=float) for _ in range(0, len(bandits))]
    for banditInd, bandit in enumerate(bandits):
        for i in range(0, nBandits):
            for t in range(0, time):
                action, reward = bandit[i].take_step()
                average_rewards[banditInd][t] += reward
                if action == bandit[i].best_action:
                    best_action_counts[banditInd][t] += 1
        best_action_counts[banditInd] /= nBandits
        average_rewards[banditInd] /= nBandits
    return best_action_counts, average_rewards


# comparison of two kinds of stpesizes in a nonstationary case
nBandits = 2000  # the number of runs
time = 1000  # the number of steps in each run
epsilon = 0.1
walk = 0.01
bandits = []
bandits.append([Bandit(epsilon=epsilon, walk=walk) for _ in range(0, nBandits)])
bandits.append([Bandit(epsilon=epsilon, stepsize=0.1, walk=walk) for _ in range(0, nBandits)])
best_action_counts, average_rewards = banditSimulation(nBandits, time, bandits)

figureIndex = 0
# plot for the numbers of best action
plt.figure(figureIndex)
figureIndex += 1
plt.plot(best_action_counts[0][10:-1], label='stepsize = 1/n')
plt.plot(best_action_counts[1][10:-1], label='stepsize = 0.1')
plt.xlabel('Steps')
plt.ylabel('optimal action')
plt.legend()
# plot for the average Rewards
plt.figure(figureIndex)
figureIndex += 1
plt.plot(average_rewards[0][10:-1], label='stepsize = 1/n')
plt.plot(average_rewards[1][10:-1], label='stepsize = 0.1')
plt.xlabel('Steps')
plt.ylabel('average reward')
plt.legend()

plt.show()

# error: typo espilon; self.actions rather than actions