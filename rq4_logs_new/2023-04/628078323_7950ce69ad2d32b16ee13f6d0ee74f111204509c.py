from population import Population
import time, os, sys, random

'''

'''

class Civilization:
    def __init__(self) -> None:
        self.break_point: float = 1
        self.list_to_try: list = [random.randint(1,10) for _ in range(100)]
        pass

    def simulation(self):
        simulation_start_time = int(time.time())
        best_fitness = 0
        index = 0
        while best_fitness < self.break_point:
            population = Population(index, 50, 2, self.list_to_try)
            if index % 5 == 0:
                population.create_population()
            else:
                population.create_new_population(last_population_results, elites)
            population.apply_fitness()
            population.ranking_sort()
            best_fitness = population.best_fitness
            self.break_point = population.max_fitness
            last_population_results = population.best_of_population
            elites = population.list_of_elites
            self.show_actual_parameters(index, simulation_start_time)
            index+=1
        else:
            print(f'Best of all:\n{population.best_of_all}')

    def show_actual_parameters(self, population_number, start_time):
        clear = lambda: os.system('cls')
        clear()
        sys.stdout.write(f'Population number: {population_number+1}\n')
        sys.stdout.write(f'Time since started: {int(time.time() - start_time)//60} minutes {int(time.time() - start_time)%60} seconds\n')
        sys.stdout.flush()

def main():
    civilization = Civilization()
    civilization.simulation()

main()