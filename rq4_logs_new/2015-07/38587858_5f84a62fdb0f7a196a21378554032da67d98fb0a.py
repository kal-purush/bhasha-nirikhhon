from .most_common import MostCommonHeuristic
from collections import defaultdict
from itertools import product
from gurobipy import GRB, Model, quicksum as sum
import sys

class BendersModel(object):
    '''Benders Decomposition of the original BIP'''
    _slug = 'benders-model'

    def slug(self):
        # TODO: presol, presol+sos1, heuristic initial sol'n
        return BendersModel._slug

    def solve(self, problem, saver):
        # TODO: time limits
        # TODO: symmetry?

        # Construct master model.
        self.problem = problem
        self.model = model = Model()
        model.params.LazyConstraints = 1

        self.theta = theta = model.addVar(lb=-GRB.INFINITY, name='theta')

        # x[i,s,c] = 1 if image i runs command c during stage s, 0 otherwise
        self.x = x = {}
        for i, cmds in problem.images.items():
            for s, c in product(problem.stages[i], cmds):
                x[i,s,c] = model.addVar(vtype=GRB.BINARY, name='x[%s,%s,%s]' % (i,s,c))

        model.update()

        # Need to reference vars by their indices later.
        self.xind = {xvar: isc for isc, xvar in x.items()}

        # Each image one command per stage, and each command once.
        for i in problem.images:
            for s in problem.stages[i]:
                model.addConstr(sum(x[i,s,c] for c in problem.images[i]) == 1)
            for c in problem.images[i]:
                model.addConstr(sum(x[i,s,c] for s in problem.stages[i]) == 1)

        model.setObjective(theta, GRB.MINIMIZE)

        # Optimize until we can longer add optimality cuts.
        iteration = 1
        while True:
            if iteration == 1:
                # Use heuristic for initial feasible solution.
                soln = []
                def save_initial(schedule):
                    soln.append(schedule)
                MostCommonHeuristic().solve(problem, save_initial)
                init = soln.pop()

                # Inform the master model of this solution.
                for i,s,c in x:
                    if init[i][s-1] == c:
                        x[i,s,c].start = 1

                def val_func(m, xvar):
                    if xvar is theta:
                        return -GRB.INFINITY

                    i,s,c = self.xind[xvar]
                    if init[i][s-1] == c:
                        return 1
                    else:
                        return 0

            else:
                model.optimize(lambda *args: self._callback(saver, *args))
                val_func = lambda m, xvar: xvar.x

            cut_func = lambda m, cons: m.addConstr(cons)
            saver(self._schedule(val_func))

            if not self._cut(model, val_func, cut_func):
                break

            iteration += 1

        saver(self._schedule(val_func))

    def _schedule(self, val_func):
        # Save schedule.
        schedule = defaultdict(list)
        for i, stages in self.problem.stages.items():
            for s in stages:
                for c in self.problem.images[i]:
                    if val_func(self.model, self.x[i,s,c]) > 0.5:
                        schedule[i].append(c)
                        break

        return schedule

    def _callback(self, saver, model, where):
        # Save incumbent solutions as they are found.
        if where != GRB.callback.MIPSOL:
            return

        val_func = lambda m, xvar: m.cbGetSolution(xvar)
        cut_func = lambda m, cons: m.cbLazy(cons)

        # Save the incumbent.
        saver(self._schedule(val_func))

        try:
            self._cut(model, val_func, cut_func)
        except Exception, e:
            print '!!!', e.errno, dir(e)

    def _cut(self, model, val_func, cut_func):
        '''Returns true if a cut was added to the master'''
        problem = self.problem
        theta = self.theta
        x = self.x

        # Create subproblem.
        sub = Model()

        # y[ip,iq,s,c] = 1 if images ip & iq have a shared path through stage
        #                s by running command c during s, 0 otherwise
        y = {}
        for (ip, iq), cmds in problem.shared_cmds.items():
            for s, c in product(problem.shared_stages[ip, iq], cmds):
                y[ip,iq,s,c] = sub.addVar(name='y[%s,%s,%s,%s]' % (ip,iq,s,c))

        sub.update()

        # Find shared paths among image pairs.
        constraints = defaultdict(list)
        for (ip, iq), cmds in problem.shared_cmds.items():
            for s in problem.shared_stages[ip,iq]:
                for c in cmds:
                    constraints[ip,s,c].append(sub.addConstr(y[ip,iq,s,c] <= val_func(model, x[ip,s,c])))
                    constraints[iq,s,c].append(sub.addConstr(y[ip,iq,s,c] <= val_func(model, x[iq,s,c])))
                if s > 1:
                    sub.addConstr(sum(y[ip,iq,s,c] for c in cmds) <= sum(y[ip,iq,s-1,c] for c in cmds))

        sub.setObjective(
            -sum(problem.commands[c] * y[ip,iq,s,c] for ip,iq,s,c in y),
            GRB.MINIMIZE
        )
        sub.optimize()

        # Add the dual prices for each variable
        pi = defaultdict(float)
        for isp, cons in constraints.iteritems():
            for c in cons:
                pi[isp] += c.pi

        # Detect optimality
        if val_func(model, theta) >= sub.objVal:
            return False # no cuts to add

        # Optimality cut
        cut_func(model, theta >= sum(pi[isp]*x[isp] for isp in pi if pi[isp]))
        return True