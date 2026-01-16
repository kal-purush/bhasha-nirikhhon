# Plans a path between waypoints using A* search algorithm
# Only finds good positions, then assumes the rake angle faces towards the next point sequentially

from astar import AStar
import numpy as np

class PathPlanner(AStar):
    def __init__(self, costmap):
        self.costmap = costmap
        self.max_x, self.max_y = self.costmap.shape[0:]

    def plan_path(self, waypoints):
        for i in range(len(waypoints)):
            self.astar()
        pass

    def plan_leg(self, waypoint_A, waypoint_B):
        foundpath = self.astar(waypoint_A, waypoint_B)
        return list(foundpath)

    def neighbors(self, node):
        # return 8-neighbors of a cell
        neighbors = [
            (node[0] - 1, node[1] - 1),
            (node[0] - 1, node[1]),
            (node[0] - 1, node[1] + 1),
            (node[0], node[1] - 1),
            (node[0], node[1] + 1),
            (node[0] + 1, node[1] - 1),
            (node[0] + 1, node[1]),
            (node[0] + 1, node[1] + 1),
        ]
        to_remove = []
        for n in neighbors:
            if n[0] < 0 or n[0] >= self.max_x:
                to_remove.append(n)
            if n[1] < 0 or n[1] >= self.max_y:
                to_remove.append(n)
        for n in to_remove:
            neighbors.remove(n)

        return neighbors

    def distance_between(self, n1, n2):
        d = abs(n1[0] - n2[0]) + abs(n1[1] - n2[1])
        return d

    def heuristic_cost_estimate(self, current, goal):
        return self.costmap[current[0], current[1]]

    def is_goal_reached(self, current, goal):
        if current[0] == goal[0] and current[1] == goal[1]:
            return True
        return False

class MapNode():
    def __init__(self):
        pass

# class SandPlan(AStar):
#     def __init__(self, costmap):
#         self.costmap = costmap