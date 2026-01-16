#######################################################################
# Copyright (C)                                                       #
# 2016 Shangtong Zhang(zhangshangtong.cpp@gmail.com)                  #
# 2016 Kenta Shimada(hyperkentakun@gmail.com)                         #
# 2017 Aja Rangaswamy (aja004@gmail.com)                              #
# Permission given to modify the code as long as you keep this        #
# declaration at the top                                              #
#######################################################################

from math import *
import itertools
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D


class Rental(object):
    """Model of car rental"""

    def __init__(self, max_cars, expect_requests, expect_returns):
        self.max_cars = max_cars  # max carnumbers
        self.requests = expect_requests  # expected request of cars
        self.returns = expect_returns  # expected returns of cars
        self.credit = 10  # rewards for renting one car
        self.cost_move = -2  # cost for moving a car
        self.cost_park = -4  # cost for parking a car
        self.max_park = [self.max_cars[0] // 2, self.max_cars[1] // 2]  # limited parking plots
        self.discount = 0.9
        self.max_move = self.max_cars[0] // 4  # maximum of moving cars
        self.actions = [_ for _ in range(-self.max_move, self.max_move + 1)]
        self.dynamics = []  # dynamics of environment
        self.init_dynamics()
        # values of action state pairs
        self.value_action = np.zeros((self.max_cars[0] + 1, self.max_cars[1] + 1, 2 * self.max_move + 1))
        self.value_state = np.zeros((self.max_cars[0] + 1, self.max_cars[1] + 1))  # value of states
        # record all states
        self.states = np.array([_ for _ in itertools.product(range(0, self.max_cars[0] + 1), range(0, self.max_cars[1] + 1))])

    def init_dynamics(self):
        """initialize dynamics"""
        for i in range(0, self.max_cars[0] + 1):
            self.dynamics.append([])
            for j in range(0, self.max_cars[1] + 1):
                self.dynamics[i].append([])
                for action in self.actions:
                    if (action >= 0 and i >= action) or (action < 0 and j >= abs(action)):
                        self.dynamics[i][j].append(self.decide([i, j], action))
                    else:
                        # nonexist states as terminal states
                        self.dynamics[i][j].append(([i, j], 0, 1))
                print([i, j])

    def decide(self, state, action):
        """Task Model"""
        # move cars, put actions moving too much cars in evaluate module
        # append return another array and donot change original
        init_cars = np.array([], dtype=int)
        init_cars = np.append(init_cars, min(state[0] - action, self.max_cars[0]))
        init_cars = np.append(init_cars, min(state[1] + action, self.max_cars[1]))
        next_states = []  # possible next states
        rewards = []  # possible rewards
        probabilities = []
        reward = 0
        # employee effect
        if action >= 1:
            reward += self.cost_move * (action - 1)
        else:
            reward += self.cost_move * abs(action)
        # park cost
        if init_cars[0] >= self.max_park[0]:
            reward += self.cost_park * (init_cars[0] - self.max_park[0])
        if init_cars[1] >= self.max_park[1]:
            reward += self.cost_park * (init_cars[1] - self.max_park[1])

        for rents in itertools.product(range(0, init_cars[0] + 1), range(0, init_cars[1] + 1)):
            # rent cars
            # initialize every time
            car_numbers_rent = np.copy(init_cars)
            probability = []
            car_numbers_rent -= np.array(rents)

            # probability for renting cars
            for i in range(0, len(rents)):
                if rents[i] != init_cars[i]:
                    probability.append(possion(rents[i], self.requests[i]))
                else:
                    # rent more cars leading to higher probability of zero state
                    probability.append(1 - np.sum([possion(_, self.requests[i]) for _ in range(0, init_cars[i])]))

            for returns in itertools.product(range(0, self.max_cars[0] - car_numbers_rent[0] + 1), range(0, self.max_cars[1] - car_numbers_rent[1] + 1)):
                # return cars
                # initialize every time
                car_numbers_return = np.copy(car_numbers_rent)
                probability_final = np.copy(probability)
                car_numbers_return += np.array(returns)

                # probability for returning cars
                for i in range(0, len(returns)):
                    if returns[i] != self.max_cars[i] - car_numbers_rent[i]:
                        probability_final[i] *= possion(returns[i], self.returns[i])
                    else:
                        # return too many cars leading to higher probability_final of full state
                        probability_final[i] *= (1 - np.sum([possion(_, self.returns[i]) for _ in range(0, self.max_cars[i] - car_numbers_rent[i])]))

                next_states.append(car_numbers_return)
                rewards.append(reward + np.sum(rents) * self.credit)
                probabilities.append(probability_final[0] * probability_final[1])
        return np.array(next_states), np.array(rewards), np.array(probabilities)

    def evaluate(self, policy, eps=1e-4):
        """evaluate policy"""
        # evaluate value of states by iteration
        while True:
            new_values = np.zeros((self.max_cars[0] + 1, self.max_cars[1] + 1))

            for i, j in self.states:
                next_states, rewards, probabilities = self.dynamics[i][j][policy[i, j] + self.max_move]

                next_values = np.array([])
                for state in next_states:
                    next_values = np.append(next_values, self.value_state[state[0], state[1]])

                new_values[i, j] = np.sum(probabilities * (rewards + self.discount * next_values))

            print('*', np.sum(np.abs(new_values - self.value_state)))
            if np.sum(np.abs(new_values - self.value_state)) < eps:
                self.value_state = new_values
                break

            self.value_state = new_values

        # evaluate value of state actions pair
        for i, j in self.states:
            for action in self.actions:
                if (action >= 0 and i >= action) or (action < 0 and j >= abs(action)):
                    next_states, rewards, probabilities = self.dynamics[i][j][action + self.max_move]
                    next_values = np.array([])
                    for state in next_states:
                        next_values = np.append(next_values, self.value_state[state[0], state[1]])

                    self.value_action[i, j, action + self.max_move] = np.sum(probabilities * (rewards + self.discount * next_values))
                else:
                    # use inf negative rewards to exclude this choice and correspond to the order
                    self.value_action[i, j, action + self.max_move] = -float('inf')
            print(self.value_action[i, j])
        print('evaluate')
        print('*****************')

        return self.value_state, self.value_action


def possion(n, lam):
    """possion distribution"""
    return pow(lam, n) * exp(-lam) / factorial(n)


class Agent(object):
    """Model of agent"""

    def __init__(self, task):
        self.task = task  # environment object to interact with
        self.policy = np.zeros((self.task.max_cars[0] + 1, self.task.max_cars[1] + 1), int)

    def improve(self):
        """Imporve policy"""
        while True:
            new_policy = np.zeros((self.task.max_cars[0] + 1, self.task.max_cars[1] + 1), int)
            value_state, value_action = self.task.evaluate(self.policy, eps=1e-2)

            for i, j in self.task.states:
                # action index is not action due to negative values
                best_action = np.argmax(value_action[i, j])
                new_policy[i, j] = self.task.actions[best_action]
                print([i, j], value_action[i, j], best_action, self.policy[i, j])

            policy_change = np.sum(new_policy != self.policy)
            print(policy_change, 'states changed')

            if policy_change == 0:
                break

            self.policy = new_policy
        return self.policy, value_state


rental = Rental([10, 10], [1.5, 2], [1.5, 1])
agent = Agent(rental)
policy, state_value = agent.improve()
for i in range(0, rental.max_cars[0] + 1):
    for j in range(0, rental.max_cars[1] + 1):
        print(policy[i, j], '|', end='')
    print()
    for j in range(0, rental.max_cars[1] + 1):
        print('--', '|', end='')
    print()
AxisXPrint = []
AxisYPrint = []
for i in range(0, 11):
    for j in range(0, 11):
        AxisXPrint.append(i)
        AxisYPrint.append(j)
# plot a policy/state value matrix
figureIndex = 0
def prettyPrint(data, labels):
    global figureIndex
    fig = plt.figure(figureIndex)
    figureIndex += 1
    ax = fig.add_subplot(111, projection='3d')
    AxisZ = []
    for i, j in rental.states:
        AxisZ.append(data[i, j])
    ax.scatter(AxisXPrint, AxisYPrint, AxisZ)
    ax.set_xlabel(labels[0])
    ax.set_ylabel(labels[1])
    ax.set_zlabel(labels[2])
prettyPrint(policy, ['# of cars in first location', '# of cars in second location', '# of cars to move during night'])
prettyPrint(state_value, ['# of cars in first location', '# of cars in second location', 'expected returns'])
plt.show()

# error: nonsensitive to variables which should be initialized every time in the loop
# especially when variables in conditions change in loops.
# write a little and test a little, use simple examples to get fast response
# Being familiar with problem means variable formats appear in mind when writing about them