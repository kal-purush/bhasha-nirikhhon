import numpy as np
import itertools
from copy import deepcopy
import random


class NKModel:
    def __init__(self, N, K, R, G, U, prob_jump=1, Beta=0.01):
        self.N = N  # Number of components
        self.K = K  # Number of interactions
        self.R = R  # Range; Number of different states that constitute the representation

        self.G = G  # Granularity; The smaller the value, the more precise the representation; For example, if the 
                    # granularity value is 2, the representation will be generated based on every 2 states (take their average values)
        
        self.U = U  # Inaccuracy; The standard deviation of the noise added to the predicted value

        self.prob_jump = prob_jump  # The probability of jumping to a random state instead of the best state

        self.Beta = Beta  # The coefficient of cost of changing the state

        self.environment = self.generate_environment()
        self.landscape = self.generate_landscape()
        self.explored = []


    def generate_environment(self):
        """Generate the coefficients for the NK landscape"""
        landscapes = np.random.randn(self.N, 2**(self.K+1))
        return landscapes

    def get_fitness(self, state):
        """Calculate the fitness of a state"""
        total_fitness = 0
        for i in range(self.N):
            # Determine the index for the sub-landscape
            index = state[i]
            for j in range(1, self.K+1):
                interaction_partner = (i + j) % self.N
                index = (index << 1) | state[interaction_partner]
            total_fitness += self.environment[i, index]
        return total_fitness / self.N

    def generate_landscape(self):
        """Generate the actual values of the entire landscape"""
        landscape = {}
        for i in range(2**self.N):
            state = [int(x) for x in f"{i:0{self.N}b}"]
            landscape[tuple(state)] = self.get_fitness(state)
        return landscape

    def generate_changed_series(self, series, num_change):
        """Generate all possible series that can be obtained by changing a specified number of elements in the series"""
        series_list = list(series)
        length = len(series_list)
        index_combinations = itertools.combinations(range(length), num_change)

        changed_series = []
        for indices in index_combinations:
            new_series = series_list[:]
            for index in indices:
                new_series[index] = 1 if new_series[index] == 0 else 0
            if new_series != list(self.initial_state):
                changed_series.append(new_series)
        
        return changed_series


    def separate_list_of_lists(self, lst, part_size):
        """Separate a list of lists into n parts"""
        n_parts = (len(lst) + part_size - 1) // part_size
        remainder = len(lst) % n_parts

        parts = []
        start = 0
        for i in range(n_parts):
            end = start + part_size + (1 if i < remainder else 0)
            start = end
            parts.append(lst[start - part_size:end])
        return parts
    

    def calculate_cost(self, state, new_state):
        """Calculate the cost of changing from the current state to the new state"""
        cost = 0
        for i in range(self.N):
            if state[i] != new_state[i]:
                cost += 1
        return cost


    def generate_representation(self, state):
        """
        Generate a representation of the fitness landscape that can be used to predict the performance of areas within the range
        """

        if self.R > 2 ** self.N:
            self.R = 2 ** self.N
        
        representation_lst = []
        representation = {}
        for i in range(0, self.N+1):
            lst = self.generate_changed_series(state, i)
            for j in lst:
                representation_lst.append(j)
                if len(representation_lst) == self.R:
                    break
        
        separate_list = self.separate_list_of_lists(representation_lst, self.G)
        for part in separate_list:
            if len(part) == 0:
                break
            average_value = sum([self.landscape[tuple(j)] for j in part]) / len(part)
            for k in part:
                representation[tuple(k)] = average_value

        return representation


    def predict_performance(self, state):
        """
        Based on the representation generated and the inaccuracy, predict the performance of the states within the range
        """
        representation_dic = self.generate_representation(state)
        predicted_dict = deepcopy(representation_dic)
        for key in predicted_dict:
            predicted_dict[key] = predicted_dict[key] * (1 + np.random.normal(0, self.U))
        return predicted_dict
    
    
    def move_agent(self, current_state):
        """calculate the best move for the agent based on the predicted performance of the states within the range"""
        best_move = [current_state[:]]
        # max_objective = self.landscape[current_state]
        max_objective = np.NINF
        cost = 0
        predicted_value = self.landscape[current_state]

        average_value = sum(self.landscape.values()) / len(self.landscape)

        # random area with representation
        state = tuple(np.random.randint(0, 2, self.N))
        while state in self.explored:
            state = tuple(np.random.randint(0, 2, self.N))
        potential_states = self.predict_performance(state)

        # compare area with representation and local ones
        for state_key, state_value in potential_states.items():
            cost_1 = self.Beta * self.calculate_cost(current_state, state_key)
            objective = state_value - cost_1
            if objective > max_objective:
                max_objective = objective
                best_move = [state_key]
                predicted_value = objective
                cost = cost_1
            elif objective == max_objective:
                best_move.append(state_key)
        
        neighbors = self.generate_changed_series(current_state, 1)
        neighbors_dic = {}
        for neighbor in neighbors:
            neighbors_dic[tuple(neighbor)] = self.landscape[tuple(neighbor)]

        if max_objective < average_value - 1 * self.Beta:
            new_state = random.choice(neighbors)
            cost = 1 * self.Beta
            predicted_value = average_value - 1 * self.Beta
        else:
            new_state = random.choice(best_move)
        
        return new_state, predicted_value, cost, neighbors_dic, average_value


    def simulate(self, iterations=10):
        """Simulate the agent moving through the landscape"""
        # Random initial state
        current_state = tuple(np.random.randint(0, 2, self.N))
        results_0 = []
        results_local = []
        results_representation = []
        self.initial_state = current_state
        self.explored.append(current_state)

        for _ in range(iterations):
            new_state, _, cost, _, _ = self.move_agent(current_state)
            actual_performance = self.get_fitness(new_state)
            result = actual_performance - cost
            if cost == 0:
                results_0.append(result)
            elif cost == 1 * self.Beta:
                results_local.append(result)
            else:
                results_representation.append(result)
            # current_state = tuple(np.random.randint(0, 2, self.N))
            current_state = new_state
            self.initial_state = current_state
            self.explored.append(current_state)

        return np.mean(results_0), len(results_0), np.mean(results_local), len(results_local), np.mean(results_representation), len(results_representation)
    

    def simulate_print(self, iterations=10):
        """simulation with print statements"""
        current_state = tuple(np.random.randint(0, 2, self.N))
        results = []
        self.initial_state = current_state

        for _ in range(iterations):
            new_state, predicted_value, cost, neighbors_dic, average_value = self.move_agent(current_state)
            actual_performance = self.get_fitness(new_state)
            result = actual_performance - cost
            results.append(result)
            print(f"Current State: {current_state}\
                  \nLocal value: {self.landscape[current_state]}\
                  \nNew State    : {new_state}\
                  \nCost: {cost}\
                  \nPredicted Value: {predicted_value}\
                  \nActual Performance: {actual_performance}\
                  \nResult: {result}\
                  \nNeighbors: {neighbors_dic}\
                  \nAverage Value: {average_value}\n")
            current_state = new_state
            self.initial_state = current_state