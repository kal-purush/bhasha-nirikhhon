from tsp_solver.algorithms import random_search, hill_climbing, simulated_annealing

cities = [(0, 0), (2, 3), (5, 4), (6, 1), (8, 3), (1, 6)]

print("Random Search:", random_search.solve_tsp(cities, iterations=1000))
print("Hill Climbing:", hill_climbing.solve_tsp(cities))
print("Simulated Annealing:", simulated_annealing.solve_tsp(cities, temp=1000, cooling_rate=0.995))