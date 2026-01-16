import collections
import time
import resource
import math
import argparse


class PuzzleState(object):
    def __init__(self, config, n, parent=None, action="Initial", cost=0):

        # if n * n != len(config) or n < 2:
        #     raise Exception("the length of config is not correct!")
        self.n = n
        self.cost = cost
        self.parent = parent
        self.action = action
        self.config = config
        self.str_config = str(config)
        self.children = []
        self.depth = parent.depth + 1 if parent else 0

        self.blank_pos = self.config.index(0)
        self.blank_row = self.blank_pos // self.n
        self.blank_col = self.blank_pos % self.n

    def display(self):
        for i in range(self.n):
            line = []
            offset = i * self.n
            for j in range(self.n):
                line.append(self.config[offset + j])
            print(line)

    def move_left(self):
        if self.blank_col == 0:
            return None
        else:
            target = self.blank_pos - 1
            new_config = list(self.config)
            new_config[self.blank_pos], new_config[target] = new_config[target], new_config[self.blank_pos]
            return PuzzleState(tuple(new_config), self.n, parent=self, action="Left", cost=self.cost + 1)

    def move_right(self):
        if self.blank_col == self.n - 1:
            return None
        else:
            target = self.blank_pos + 1
            new_config = list(self.config)
            new_config[self.blank_pos], new_config[target] = new_config[target], new_config[self.blank_pos]
            return PuzzleState(tuple(new_config), self.n, parent=self, action="Right", cost=self.cost + 1)

    def move_up(self):
        if self.blank_row == 0:
            return None
        else:
            target = self.blank_pos - self.n
            new_config = list(self.config)
            new_config[self.blank_pos], new_config[target] = new_config[target], new_config[self.blank_pos]
            return PuzzleState(tuple(new_config), self.n, parent=self, action="Up", cost=self.cost + 1)

    def move_down(self):
        if self.blank_row == self.n - 1:
            return None
        else:
            target = self.blank_pos + self.n
            new_config = list(self.config)
            new_config[self.blank_pos], new_config[target] = new_config[target], new_config[self.blank_pos]
            return PuzzleState(tuple(new_config), self.n, parent=self, action="Down", cost=self.cost + 1)

    def expand(self, reverse = False):
        # add child nodes in order of UDLR
        if len(self.children) == 0:
            tmp_children = [
                self.move_up(),
                self.move_down(),
                self.move_left(),
                self.move_right()
            ]
            self.children = [x for x in tmp_children if x is not None]
            if reverse:
                self.children = list(reversed(self.children))
        return self.children


def bfs_search(initial_state):
    """
    BFS search
    """
    goal = tuple(range(len(initial_state.config)))
    frontier = collections.deque([initial_state])
    explored = []
    success = None
    max_search_depth = 0

    while len(frontier) > 0:
        state = frontier.popleft()
        if state.depth > max_search_depth:
            max_search_depth = state.depth
        explored.append(state.config)

        if test_goal(state, goal):
            frontier.clear()
            success = state
        else:
            for child in state.expand():
                if child.config not in explored \
                        and child.config not in list(map(lambda x: x.config, frontier)):
                    frontier.append(child)

    return dict(end_state=success, n_expanded=len(explored)-1, max_search_depth=max_search_depth)


def dfs_search(initial_state):
    """
    DFS search
    """
    goal = tuple(range(len(initial_state.config)))
    frontier = [initial_state]
    frontier_config = [initial_state.str_config]
    explored = []
    success = None
    max_search_depth = 0

    while len(frontier) > 0:
        state = frontier.pop()
        frontier_config.pop()
        if state.depth > max_search_depth:
            max_search_depth = state.depth
            if (max_search_depth % 1000) == 0:
                print(f"{max_search_depth}")
        explored.append(state.config)

        if test_goal(state, goal):
            frontier.clear()
            success = state
        else:
            for child in state.expand(reverse=True):
                if child.config not in explored and \
                        child.str_config not in frontier_config:
                    frontier.append(child)
                    frontier_config.append(child.str_config)

    return dict(end_state=success, n_expanded=len(explored)-1, max_search_depth=max_search_depth)


def A_star_search(initial_state):
    """A * search"""



def calculate_total_cost(state):
    """calculate the total estimated cost of a state"""



def calculate_manhattan_dist(idx, value, n):
    """calculate the manhattan distance of a tile"""


def test_goal(puzzle_state, goal):
    """test the state is the goal state or not"""
    return puzzle_state.config == goal


# Function that Writes to output.txt
def write_output(result, start_time):
    def rewind(state, path=None):
        if not path:
            path = []
        if state.parent is not None:
            path.insert(0, state.action)
            rewind(state.parent, path)
        return path

    end_state = result['end_state']
    end_state.display()
    path = rewind(end_state)
    print(f"path_to_goal: {path}")
    print(f"cost_of_path: {end_state.cost}")
    print(f"nodes_expanded: {result['n_expanded']}")
    print(f"max_search_depth: {result['max_search_depth']}")
    end_time = time.time()
    r = resource.getrusage(resource.RUSAGE_SELF)
    print(f"running_time: {end_time - start_time}")
    print(f"max_ram_usage: {r.ru_maxrss/1024/1024}")


def process(strategy, init_state, start_time):
    start_state = tuple(map(int, init_state))
    size = int(math.sqrt(len(start_state)))
    hard_state = PuzzleState(start_state, size)

    if strategy == "bfs":
        result = bfs_search(hard_state)
    elif strategy == "dfs":
        result = dfs_search(hard_state)
    elif strategy == "ast":
        result = A_star_search(hard_state)
    else:
        raise NotImplementedError(f"Unknown search strategy: {sm}")

    if result['end_state'] is not None:
        write_output(result, start_time)
    else:
        raise Exception("Unable to find solution")


if __name__ == '__main__':
    m_start_time = time.time()
    parser = argparse.ArgumentParser(description='Solve 8-puzzle.')
    parser.add_argument('sm', metavar='SM', help='Search model', choices=['bfs', 'dfs', 'ast'])
    parser.add_argument('start_state', metavar='INIT', help='Starting state')
    args = parser.parse_args()

    sm = args.sm
    m_start_state = args.start_state.split(",")
    process(sm, m_start_state, m_start_time)