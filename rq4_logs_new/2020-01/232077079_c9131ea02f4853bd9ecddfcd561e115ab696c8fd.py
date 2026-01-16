from queue import PriorityQueue
import numpy as np
import time

def heuristic(current, goal):
    """
    Calculate heuristic distance between the current node and end node.
    """

    h = abs(np.array(current) - np.array(goal))
    return h.sum()

def neighbour_add(neighbours, current, moves, grid, path):
    """
    Add neighbours depending on different conditions
    """

    for i in moves:
        neighbour = tuple(np.array(current) + np.array(i))
        if neighbour not in path:
            if neighbour in grid:
                if grid.get(neighbour)[0]:
                    neighbours.append(neighbour)
    return neighbours

def gate_neighbours(current, grid, path):
    """
    Generate a heatmap for gates so that A star can avoid gates. This consists of all direct neighbours, 
    diagonal neighbours, and every coordinate straight above the gate in question
    """

    neighbours = list()

    # Generate all other coordinates around the gate that Astar should avoid
    moves = list()
    for x in range(-1, 2):
        for y in range(-1, 2):
            for z in range(8):
                moves.append((x, y, z))
    
    return neighbour_add(neighbours, current, moves, grid, path)

def neighbours(current, grid, path):
    """
    Generate all direct legal(traversable) neighbours for the current node
    """

    neighbours = list()

    # All direct moves in a list
    moves = [(1, 0, 0), (-1, 0, 0), (0, 1, 0), (0, -1, 0), (0, 0, 1), (0, 0, -1)]

    return neighbour_add(neighbours, current, moves, grid, path)

def finished_check(current, end):
    if current == end:
        return True
    return False

def a_star(start, end, grid):
    """
    A star search function: It takes a start node, end node, grid, and a ceiling_count
    """

    # Initialize the priority queue
    pq = PriorityQueue()

    # Set the path with only the start node in it
    path = [start]
    
    visited = set()

    f = grid.get(path[-1])[1] + heuristic(path[-1], end)

    # Put the path with its F score in the queue
    pq.put((f, path))

    # Make sure the algorithm goes through the whole queue
    while not pq.empty():

        # Get the next prioritized item based on its F
        path = pq.get()[1]

        # Set the last node in the path to the current node
        current = path[-1]

        # Check if the program finished succesfully
        if finished_check(current, end):
            return path

        for i in neighbours(current, grid, path):

            # Calculate the F for each neighbour
            new_path = path + [i]

            # G will also include the extra weight 
            g = len(new_path) + grid.get(i)[1]
            f = g + heuristic(i, end)

            if i not in visited:
                pq.put((f, new_path))
                visited.add(i)

    # Return false if the queue is empty and no solution has been found
    return False

def a_star_basic(start, end, grid):
    """
    A basic version of the A star algorithm without any added heuristics
    """

    pq = PriorityQueue()
    path = [start]
    f = 0 + heuristic(path[-1], end)

    visited = set()

    pq.put((f, path))

    while not pq.empty():
        path = pq.get()[1]
        current = path[-1]

        if finished_check(current, end):
            return path

        for i in neighbours(current, grid, path):
            new_path = path + [i]
            f = len(new_path) + heuristic(i, end)
            if i not in visited:
                pq.put((f, new_path))
                visited.add(i)

    return False


def make_grid(gates):
    """
    Generate a grid that can be used my the A star search algorithm. It has an added heuristic function
    causing A star to avoid lower layers.
    """ 

    # Change grid size based on the netlist
    if len(gates) == 25:
        y_range = 13
    else:
        y_range = 17

    grid = {}

    for x in range(18):
        for y in range(y_range):
            for z in range(8):
                # Make lower layers more expensive
                g = (8 - z) * 2

                # In the grid dictionary: each key is a coordinate, each value is a list of a boolean and a G score
                grid[(x, y, z)] = [True, g]
     
    return grid