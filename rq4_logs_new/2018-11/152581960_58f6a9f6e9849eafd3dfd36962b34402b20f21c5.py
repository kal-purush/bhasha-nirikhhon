# use z3 in order to find good tours
from z3 import *

class Tour(list):
    pass

class TourSolver():
    def __init__(self):
        self.pois = []
        self._solver = Solver()
        self._poi_variables = {}
        self._order_variables = {}

    def add_poi(self, poi):
        self.pois[poi["ID"]] = poi
        self._poi_variables[poi["ID"]] = Int(f'x_{poi["ID"]}')

    # Input: length in km
    def restrict_tour_length(self, length):
        # the sum of all parts of the tour must be less than length
        assert(false) # not implemented

    def make_poi_mandatory(self, poi_id):
        assert(false)

    def make_poi_start(self, poi_id):
        assert(false)

    def solve(self):
        self._build_ip()
        # important! first check, otherwise we segfault
        sat_result = self._solver.check()

        if sat_result == unsat:
            return Tour()

        # extract the model
        model = self._solver.model()

        # iterate through model and create the Tour
        tour = Tour()
        for pid, var in self._poi_variables.items():
            for other_pid, other_var in self._poi_variables.items():
                if model[self._order_variables[pid, other_pid]] > 0:
                    # we choose to go from pid to other_pid
                    tour.append(self.pois[(pid, other_pid)])


    def clear(self):
        # just replace the solver by a new instance
        self._solver = Optimize()

    def _build_ip(self):
        # add variables
        for pid, var in self._poi_variables.items():
            for other_pid, other_var in self._poi_variables.items():
                if pid != other_pid:
                    self._order_variables[pid, other_pid] = Int(f'y_{pid}_{other_pid}')

        # sum of all outgoing edges to i must be equal to x_i
        for pid, var in self._poi_variables.items():
            constr = 0
            for other_pid, other_var in self._poi_variables.items():
                if pid != other_pid:
                    contr = constr + y[pid, other_pid]

            self._solver.add(constr == pid)

        # sum of all incoming edges to i must be equal to x_i
        for pid, var in self._poi_variables.items():
            constr = 0
            for other_pid, other_var in self._poi_variables.items():
                if pid != other_pid:
                    contr = constr + y[other_pid, pid]

            self._solver.add(constr == pid)

        # probably we will never need this - it just holds the objective value
        self._objective = self._solver.maximize()
