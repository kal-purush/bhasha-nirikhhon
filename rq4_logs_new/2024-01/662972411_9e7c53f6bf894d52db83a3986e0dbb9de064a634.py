import numpy as np
from shapely import Polygon

from Algorithms.BranchesAndBounds.Branch import Branch
from Algorithms.BranchesAndBounds.ParamsClasses.FlexibleBnBParams import FlexibleBnBParams
from Utils.Circle import Circle


class FlexibleBnBAlgorithm:
    def __init__(self,
                 P: Polygon,
                 circles: list[Circle]):
        self.P = P
        self.circles = circles

    def set_params(self,
                   max_iterations: int,
                   params: FlexibleBnBParams,
                   fixed: list[int] = None):
        self.max_iterations = max_iterations
        self.params = params

        if fixed is None:
            self.fixed = [0] * len(self.circles)
        else:
            self.fixed = fixed

    def run_algorithm(self):
        self.params.reset()
        main_branch = Branch(self.P, self.circles, self.fixed)
        self.params.calculate_metric([main_branch])

        best_branch = main_branch

        iterations = self.max_iterations
        while iterations > 0 and len(main_branch.circles) > 0:
            bad_inds = self.params.find_bad_circles(main_branch)
            branches = self.params.create_branches(main_branch, bad_inds)
            self.params.calculate_metric(branches)
            cur_best = np.argmax([branch.metric for branch in branches])
            main_branch = branches[cur_best]
            if main_branch.metric > best_branch.metric:
                best_branch = main_branch
            iterations -= 1

        self.circles = best_branch.circles
        self.fixed = best_branch.fixed

    def get_result(self):
        return self.circles