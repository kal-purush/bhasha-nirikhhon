import time

import matplotlib

from crossovers import *
from genetic import *
from mutations import *
from services import set_up_primary_plot

matplotlib.use('TkAgg')


class Action:
    def __init__(self, population, epoch, selection, crossover, percent_best, tournament_size, mutation,
                 mutation_probability,
                 crossover_probability, strategy, strategy_size) -> None:
        self.f = lambda x, y: ((1.5 - x + x * y) ** 2 + (2.25 - x + x * y ** 2) ** 2 + (2.625 - x + x * y ** 3) ** 2)
        self.fx = []
        self.fxSum = 0
        self.fxAverage = []
        self.fstd = []
        self.ix = 0
        self.population = population
        self.epoch = epoch
        self.selection = selection
        self.crossover = crossover
        self.percent_best = percent_best
        self.tournament_size = tournament_size
        self.mutation = mutation
        self.mutation_probability = mutation_probability
        self.crossover_probability = crossover_probability
        self.strategy = strategy
        self.strategy_size = strategy_size
        self.best_x = []
        self.best_y = 0

    def create_file(self, selection):
        timestr = time.strftime("%Y%m%d-%H_%M_%S")
        filename = "saveResults/" + selection + "-" + timestr
        file = open(filename, "w")
        file.write("Iteration   genPopulation[0]        resultFunction\n")
        return file

    def get_crossover_type(self):
        if self.crossover == 1:
            return "Arithmetic Crossover"
        elif self.crossover == 2:
            return "Heuristic Crossover"

    def get_crossover(self):
        if self.crossover == 1:
            crossover = ArithmeticCrossover(self.crossover_probability)
            return crossover
        elif self.crossover == 2:
            crossover = HeuristicCrossover(self.crossover_probability, 5)
            return crossover

    def get_mutation(self):
        if self.mutation == 1:
            mutation = IndexChangeMutation(self.mutation_probability)
            return mutation
        elif self.mutation == 2:
            mutation = RegularMutation(self.mutation_probability)
            return mutation

    def get_selection(self):
        if self.selection == 1:
            return IRouletteWheelSelection
        elif self.selection == 2:
            return ITournamentSelection
        elif self.selection == 3:
            return IBestSelection

    def go(self):
        file = self.create_file(self.get_selection())
        ix = 0
        start = time.time()

        crossover_type = self.get_crossover_type()
        genetic = Genetic(self.f, self.get_selection(), self.get_mutation(), self.get_crossover(), self.population,
                          group_size=self.tournament_size,
                          percent_best=self.percent_best, strategy_size=self.strategy_size)

        while ix < self.epoch:
            ix += 1
            winners = genetic.make_selection()
            genetic.make_crossover(winners)
            self.fx.append(self.f(genetic.population[0][0], genetic.population[0][1]))
            self.fxSum += self.f(genetic.population[0][0], genetic.population[0][1])
            self.fxAverage.append(self.fxSum / ix)
            self.fstd.append(np.std(self.fxAverage))
            file.write(str(ix) + "       " + str(genetic.population[0]) + "     " + str(
                self.f(genetic.population[0][0], genetic.population[0][1])) + " \n")
        ix = 0
        end = time.time()
        duration = end - start
        self.set_best_x(genetic.population[0])
        self.set_best_y(self.f(genetic.population[0][0], genetic.population[0][1]))
        self.add_info_to_file(crossover_type, duration, file)
        file.close()
        print(duration)
        print(genetic.population[0])
        x = np.arange(0, self.epoch, 1)
        set_up_primary_plot(x, self.fx, self.fstd, self.fxAverage, self.get_selection())
        self.fx.clear()
        self.fstd.clear()
        self.fxAverage.clear()
        self.fxSum = 0
        x = 0
        return duration

    def add_info_to_file(self, crossover_type, duration, file):
        file.write("\nInformation about execution: \n")
        file.write("    Total time: " + str(duration) + "\n")
        file.write("    Crossover: " + crossover_type + "\n")
        file.write("    Epoch: " + str(self.epoch) + "\n")
        file.write("    Population: " + str(self.population) + "\n")
        file.write("    ProbabilityMutation (%): " + str(self.mutation_probability) + "\n")
        file.write("    ProbabilityCrossOver (%): " + str(self.crossover_probability) + "\n")
        file.write("    EliteStrategyAmount: " + str(self.strategy_size) + "\n")
        file.write("    x = " + str(self.get_best_x()) + "\n")
        file.write("    f(x) = " + str(self.get_best_y()) + "\n")
        if self.get_selection() == ITournamentSelection:
            file.write("    TournamentSize: " + str(self.tournament_size) + "\n")
        if self.get_selection() == IBestSelection:
            file.write("    PercentBest: " + str(self.percent_best * 100) + "%\n")

    def set_best_x(self, best_x):
        self.best_x = best_x

    def get_best_x(self):
        return self.best_x

    def set_best_y(self, best_y):
        self.best_y = best_y

    def get_best_y(self):
        return self.best_y