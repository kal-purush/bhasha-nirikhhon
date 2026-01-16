from collections import defaultdict
from itertools import product
from gurobipy import GRB, Model, quicksum as sum

class BIPModel(object):
    '''Reference binary integer program: full model with no decomposition'''
    _slug = 'bip-model'

    def slug(self):
        # TODO: presol, presol+sos1, linear 2nd stage, heuristic initial sol'n
        # TODO: benders, benders+sos1+presol, +heuristic initial sol'n
        # TODO: sos1 will not create a full schedule, only a shared schedule
        return BIPModel._slug

    def solve(self, problem):
        # TODO: time limits

        # Construct model.
        self.problem = problem
        self.model = model = Model()

        # x[i,s,c] = 1 if image i runs command c during stage s, 0 otherwise
        self.x = x = {}
        for i, cmds in problem.images.items():
            for s, c in product(problem.stages[i], cmds):
                x[i,s,c] = model.addVar(vtype=GRB.BINARY, name='x[%s,%s,%s]' % (i,s,c))

        # y[ip,iq,s,c] = 1 if images ip & iq have a shared path through stage
        #                s by running command c during s, 0 otherwise
        y = {}
        for (ip, iq), cmds in problem.shared_cmds.items():
            for s, c in product(problem.shared_stages[ip, iq], cmds):
                y[ip,iq,s,c] = model.addVar(vtype=GRB.BINARY, name='y[%s,%s,%s,%s]' % (ip,iq,s,c))

        model.update()

        # Each image one command per stage, and each command once.
        for i in problem.images:
            for s in problem.stages[i]:
                model.addConstr(sum(x[i,s,c] for c in problem.images[i]) == 1)
            for c in problem.images[i]:
                model.addConstr(sum(x[i,s,c] for s in problem.stages[i]) == 1)

        # Find shared paths among user pairs.
        for (ip, iq), cmds in problem.shared_cmds.items():
            for s in problem.shared_stages[ip,iq]:
                for c in cmds:
                    model.addConstr(y[ip,iq,s,c] <= x[ip,s,c])
                    model.addConstr(y[ip,iq,s,c] <= x[iq,s,c])
                if s > 1:
                    model.addConstr(sum(y[ip,iq,s,c] for c in cmds) <= sum(y[ip,iq,s-1,c] for c in cmds))

        model.setObjective(sum(y.values()), GRB.MAXIMIZE)
        model.optimize()

        # TODO: callback to collect solutions along the way

        # Create optimal schedule
        schedule = defaultdict(list)
        for i, stages in problem.stages.items():
            for s in stages:
                for c in problem.images[i]:
                    if x[i,s,c].x > 0.5:
                        schedule[i].append(c)
                        break

        return [schedule]