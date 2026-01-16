###########################################################################
# A* search algorithm
# 
# By team De Mandarijntjes
# 
# Pseudocode from http://mat.uab.cat/~alseda/MasterOpt/AStar-Algorithm.pdf
#             and https://en.wikipedia.org/wiki/A*_search_algorithm
###########################################################################

# function reconstruct_path(cameFrom, current)
#     total_path := {current}
#     while current in cameFrom.Keys:
#         total_path.prepend(current)
#         current := cameFrom[current]
#     return total_path


def heuristic(currentCell, goal):
    return abs(currentCell.x - goal.x) + abs(currentCell.y - goal.y)

class Node():
    def __init__(self, x, y):
        self.x = x
        self.y = y
    
    def f_score(self):
        h = self.h_score()
        return self.g + h
    
    def g_score(self, value):
        self.g = value
        return self.g
    
    def h_score(self):
        self.h = heuristic(Node(self.x, self.y), Goal(1, 1))
        
        return self.h
    
    def successors(self, grid, node_current):
        node_successors = []
        for i in grid:
            for j in grid:
                # Kan mooier?
                if i.x == node_current.x and i.y == node_current.y:
                    if abs(j.x - i.x) == 1 and j.y - i.y == 0:    
                        if (j,i) not in node_successors:
                            node_successors.append((j))
                    elif abs(j.y - i.y) == 1 and j.x - i.x == 0:
                        if (j,i) not in node_successors:
                            node_successors.append((j))

        return node_successors
    

class Goal():
    def __init__(self, x, y):
        self.x = x
        self.y = y

# class Gscore():
#     def __init__(self, node):
        



grid = []

grid_size = 2
for x in range(grid_size):
    for y in range(grid_size):
        grid.append(Node(x, y))


open_list = []
closed_list = []

node_start = Node(0, 0)
node_goal = Node(1, 1)

open_list.append(node_start)

node_start.g_score(0)

# weight of move, could be used to indicated another wire or gate with extremely high value
w = 1
while open_list:
    # for i in open_list:
    #     print(i.x, i.y)

    # Take the node with the lowest f TODO
    node_current =  open_list[0]

    # whereami
    # Take from the open list the node node_current with the lowest f

    if node_current == node_goal:
        print("Found solution")
        break

    for i in node_start.successors(grid, node_current):
        # print(i.x, i.y)
        successor_current_cost = node_current.g + w
        
        if i in open_list:
            if i.g <= successor_current_cost:
                break
        elif i in closed_list:
            if i.g <= successor_current_cost:
                break
            # in the above if statement?
            closed_list.remove(i)
            open_list.append(i)
        else:
            open_list.append(i)
            i.h_score()
        i.g_score(successor_current_cost)
        node_current = i

    closed_list.append(node_current)

if node_current != node_goal:
    print("error, open_list is empty!")
else:
    print("success?")
    

    



