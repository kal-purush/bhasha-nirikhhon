import copy
import time
from Algorithm.Generators.Graph_Generator import Graph_Generator
from Algorithm.Algorithms.Solver_Master import Solve_Master
import Algorithm.Output.Out_Data_Graph as out


class Algorithm_Master:

    def __init__(self):
        # Generator of graphs
        self.graph_generator = Graph_Generator()

        # Solver of graph
        self.solver_master = Solve_Master()

    def solve_ftlrp_problem(self, data):
        n_node = int(data['n_node'])
        max_travel = int(data['max_travel'])
        min_dist = int(data['min_dist'])
        max_dist = int(data['max_dist'])
        p_link = float(data['prob_link'])
        solution = self.graph_generator.generate_graph(n_node, p_link,max_travel,min_dist,max_dist)
        start_time = time.time()
        self.solver_master.solve_greedy(solution)
        end_time = time.time()

        # Gets how many time in seconds it needed
        solution.time_to_solve = end_time - start_time

        # Show graphic of the graph
        return solution
