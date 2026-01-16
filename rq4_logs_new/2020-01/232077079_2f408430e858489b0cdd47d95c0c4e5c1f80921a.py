##################################################
# Astar_2.py
# 
# By team De Madarijntjes
# 
# Pseudocode source: http://mat.uab.cat/~alseda/MasterOpt/AStar-Algorithm.pdf
##################################################
class Grid():
    def __init__(self):
        self.grid = []
        self.g = 0

    def make_grid(self, blocked, start, goal, net):
        for x in range(-1, 17):
            for y in range(-1, 12):
                for z in range(8):
                    node = Node(x, y, z, net)
                    # if str(node) not in blocked:
                    #     self.grid.append(node)
                    # else: 
                    #     self.grid.append(node).
                    self.grid.append(node)
                    if str(node) in blocked and str(node) != str(start) and str(node) != str(goal):
                        node.set_blocked()
    
    

    def get_grid(self):
        return self.grid
        

class Node():
    def __init__(self, x, y, z, net):
        self.x = x
        self.y = y
        self.z = z
        self.net = net
        self.g = 0
        self.blocked = False
        self.parent = None

    def get_blocked(self):
        return self.blocked

    def set_blocked(self):
        self.blocked = True
        
    def f_score(self):
        return self.g + self.h

    def get_coordinate(self):
        return str([self.x, self.y, self.z])
    
    def get_g(self):
        return self.g
    
    def set_g(self, value):
        self.g = value
    
    def g_score(self, value):
        self.g += value
    
    def h_score(self, current, goal):
        self.h = abs(current.x - goal.x) + abs(current.y - goal.y) + abs(current.z - goal.z)
        
        return self.h
    
    def set_parent(self, node):
        self.parent = node
    
    def get_parent(self):
        return self.parent

    def successors(self, grid, node_current, closed_list, blocked, start, goal):
        self.node_successors = set()
        
        # grid maken waar blocked niet in zit 

        for i in grid:
            for j in grid:
                if i.x == node_current.x and i.y == node_current.y and i.z == node_current.z:
                    if abs(j.x - i.x) == 1 and j.y - i.y == 0 and j.z - i.z == 0:   
                        if j not in closed_list and str(j) != str(start):
                            if not j.get_blocked():
                                self.node_successors.add(j)
                    elif abs(j.y - i.y) == 1 and j.x - i.x == 0 and j.z - i.z == 0:
                        if j not in closed_list and str(j) != str(start):
                            if not j.get_blocked():
                                self.node_successors.add(j)
                    elif abs(j.z - i.z) == 1 and j.x - i.x == 0 and j.y - i.y == 0:
                        if j not in closed_list and str(j) != str(start):
                            if not j.get_blocked():
                                self.node_successors.add(j)
       
        # print("before delete: ", self.node_successors)
        
        
        # coordinates = []
        # for coordinate in blocked: 
        #     coordinates.append(coordinate.get_coordinate())

        # for successor in self.node_successors:
        #     if successor.get_coordinate() in coordinates:
        #         if successor.get_coordinate() == [2, 1, 0]:
        #             print('check', coordinates)
        #             # input()
        #         if str(successor) != str(start) and str(successor) != str(goal):
        #             print(successor.get_coordinate())
        #             self.node_successors.remove(successor)
        # print("BEFORE: ", self.node_successors)

        # self.node_successors = self.node_successors - set(blocked)
        # print("AFTER: ", self.node_successors)
        # print(coordinates)
        # input('end')
        return self.node_successors

    def __repr__(self):
        return str([self.x, self.y, self.z])


def search(open_list, closed_list, blocked, grid, start, goal):
    # print(open_list, closed_list, blocked, start, goal)
    # input()
    while open_list:
        f_list = []
        for i in open_list:
            f_list.append(i.f_score())
        minimum = min(f_list)
        for i in range(len(f_list)):
            if f_list[i] == minimum:
                q = open_list[i]

        
        if str(q) == str(goal):
            print("finished")
            break

        successors = q.successors(grid, q, closed_list, blocked, start, goal)
        # print("Q", q, successors)

        # print("successssss", q, successors)
        
        # node_previous = q
                    
        # print("current: ", q)
        for i in successors:

            # Set successor_current_cost = g(node_current) + w(node_current, node_successor)
            successor_current_cost = q.get_g() + 1
            if i in open_list:
                if i.get_g() <= successor_current_cost:
                    i.set_g(successor_current_cost)
                    i.set_parent(q)    

            elif i in closed_list:
                if i.get_g() <= successor_current_cost:
                    # print("2")
                    break
                # print("3")
                closed_list.remove(i)
                open_list.append(i)
            
            else:
                # print("4")
                open_list.append(i)
                i.h_score(i, goal)
            # A star werkt niet als alles dezelfde kost heeft toch een stap terug moet extra kosten hebben doordat de huristiek daar groter wordt.
            # print("plus 1:::: ", successor_current_cost, i.h, i)
            
            i.set_g(successor_current_cost)
            i.set_parent(q)    
         
        open_list.remove(q)
        closed_list.append(q)
        
    return q

def generate_path(crd):
    path = [crd]

    while crd.get_parent() != None:
        crd = crd.get_parent()
        path.append(crd)

    return path

def initialize_grid(blocked, start, goal, net):
    grid = Grid()
    grid.make_grid(blocked, start, goal, net)
    return grid.get_grid()

def a_star(start, goal, blocked, net):
    # Set start and goal
    start = Node(start[0], start[1], start[2], net)
    goal = Node(goal[0], goal[1], goal[2], net)
    print(start, goal)

    # Initialize the grid for program to run on
    grid = initialize_grid(blocked, start, goal, net)

    # Calculate h for start node
    start.h_score(start, goal)

    print(start.h)

    # Create lists to keep track of open nodes, closed nodes, and previous nodes for optimal path
    open_list = [start]

    closed_list = []
    q =  search(open_list, closed_list, blocked, grid, start, goal)
    
    return generate_path(q)

# print(a_star())