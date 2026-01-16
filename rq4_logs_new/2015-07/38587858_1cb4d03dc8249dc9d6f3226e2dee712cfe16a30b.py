from collections import defaultdict

class MostTimeHeuristic(object):
    '''Heuristic that shares the most time consuming command at any point'''
    _slug = 'most-time'

    def slug(self):
        return MostTimeHeuristic._slug

    def solve(self, problem):
        # Keep track of what hasn't been assigned and how many of each thing there are.
        remaining = {i: set(problem.images[i]) for i in problem.images}
        order = defaultdict(list)
        self._assign(problem, remaining, order)
        return [order]

    def _assign(self, problem, remaining, order):
        if not remaining:
            return

        # Figure the most common command.
        by_cmd = defaultdict(set)
        for i, cmds in remaining.items():
            for c in cmds:
                by_cmd[c].add(i)

        most_time = max(by_cmd, key=lambda c: len(by_cmd[c]) * problem.commands[c])

        # Add this to the schedule for any it applies to.
        new_remain = {}
        for i in by_cmd[most_time]:
            order[i].append(most_time)
            remaining[i].remove(most_time)
            if remaining[i]:
                new_remain[i] = set(remaining[i])
            del remaining[i]

        self._assign(problem, new_remain, order)
        self._assign(problem, remaining, order)