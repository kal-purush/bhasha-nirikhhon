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


class Bandit(object):
    """Model of the k-armed Bandit Problem"""

    def __init__(self, kArm=10, epsilon=0, initial=0, stepSize=0.1):
        self.k = kArm  # number of the arms
        self.stepSize = stepSize  # constant step for updating the estimate
        self.indices = np.arange(self.k)
        self.qTrue = []  # value for each action
        self.qEst = np.zeros(self.k)  # estimate for each action's value
        self.averageReward = 0  # average of the total reward in one run
        self.actionCount = []  # record the number of each action being chosen
        self.epsilon = epsilon  # probability for exploration
        self.time = 0  # rounds for the trial

        # initialize real rewards with N(0,1) and estimations with initial
        for i in range(0, self.k):
            self.qTrue.append(np.random.randn())
            self.qEst[i] = initial
            self.actionCount.append(0)

        # get action with highest value
        self.bestAction = np.argmax(self.qTrue)

    def getAction(self):
        """the action for this bandit, explore or exploit?"""
        # explore
        if self.epsilon > 0:
            if np.random.binomial(1, self.epsilon) == 1:
                np.random.shuffle(self.indices)
                return self.indices[0]

        # exploit
        return np.argmax(self.qEst)

    def takeAction(self, action):
        """take the action and update the estimates for each action"""
        # generate the reward
        reward = np.random.randn() + self.qTrue[action]
        self.time += 1
        self.averageReward = (self.time - 1) / self.time * \
            self.averageReward + reward / self.time
        self.actionCount[action] += 1
        # use the sample average to update the estimate
        self.qEst[action] += (reward - self.qEst[action]
                              ) / self.actionCount[action]
        return reward


def banditSimulation(nBandits, time, bandits):
    """Play the bandits with nBandits runs each has time steps"""
    bestActionCounts = [np.zeros(time, dtype=float)
                        for _ in range(0, len(bandits))]
    averageRewards = [np.zeros(time, dtype=float)
                      for _ in range(0, len(bandits))]
    cumlativeRewards = [np.zeros(time, dtype=float)
                        for _ in range(0, len(bandits))]
    for banditInd, bandit in enumerate(bandits):
        for i in range(0, nBandits):
            for t in range(0, time):
                action = bandit[i].getAction()
                reward = bandit[i].takeAction(action)
                averageRewards[banditInd][t] += reward
                if action == bandit[i].bestAction:
                    bestActionCounts[banditInd][t] += 1
        bestActionCounts[banditInd] /= nBandits
        averageRewards[banditInd] /= nBandits
        # here we calculate the cumlative properties
        for j in range(0, time):
            cumlativeRewards[banditInd][j] =averageRewards[banditInd][j] + cumlativeRewards[banditInd][j - 1]
    return bestActionCounts, averageRewards, cumlativeRewards


nBandits = 2000  # the number of runs
time = 1000  # the number of steps in each run
# Coparison of epsilon-greedy action-value methods
epsilons = [0, 0.01, 0.1]
bandits = []
for epsInd, eps in enumerate(epsilons):
    bandits.append([Bandit(epsilon=eps) for _ in range(0, nBandits)])
bestActionCounts, averageRewards, cumlativeRewards = banditSimulation(nBandits, time, bandits)
figureIndex = 0
# plot for the numbers of best action
plt.figure(figureIndex)
figureIndex += 1
for eps, counts in zip(epsilons, bestActionCounts):
    plt.plot(counts, label='$\epsilon$ = %r' % eps)
plt.xlabel('Steps')
plt.ylabel('optimal action')
plt.legend()
# plot for the average Rewards
plt.figure(figureIndex)
figureIndex += 1
for eps, rewards in zip(epsilons, averageRewards):
    plt.plot(rewards, label='$\epsilon$ = %r' % eps)
plt.xlabel('Steps')
plt.ylabel('average reward')
plt.legend()
# plot for the culmative rewards
plt.figure(figureIndex)
figureIndex += 1
for eps, cumrewards in zip(epsilons, cumlativeRewards):
    plt.plot(cumrewards, label='$\epsilon$ = %r' % eps)
plt.xlabel('Steps')
plt.ylabel('cumlative reward')
plt.legend()


plt.show()