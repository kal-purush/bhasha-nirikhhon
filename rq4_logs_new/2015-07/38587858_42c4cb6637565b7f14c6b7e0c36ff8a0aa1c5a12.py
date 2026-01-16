from collections import defaultdict

class MostCommonHeuristic(object):
    '''Heuristic that shares the most common shared command at any point'''
    _slug = 'most-common'

    def slug(self):
        return MostCommonHeuristic._slug

    def solve(self, problem):
        # Keep track of what hasn't been assigned and how many of each thing there are.
        remaining = {i: set(problem.images[i]) for i in problem.images}
        order = defaultdict(list)
        self._assign(remaining, order)
        return [order]

    def _assign(self, remaining, order):
        if not remaining:
            return

        # Figure the most common command.
        by_cmd = defaultdict(set)
        for i, cmds in remaining.items():
            for c in cmds:
                by_cmd[c].add(i)

        most_common = max(by_cmd, key=lambda p: len(by_cmd[p]))

        # Add this to the schedule for any it applies to.
        new_remain = {}
        for i in by_cmd[most_common]:
            order[i].append(most_common)
            remaining[i].remove(most_common)
            if remaining[i]:
                new_remain[i] = set(remaining[i])
            del remaining[i]

        self._assign(new_remain, order)
        self._assign(remaining, order)