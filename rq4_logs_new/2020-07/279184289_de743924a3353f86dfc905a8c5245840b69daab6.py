import random

#
# l = [0,1,5]
# w = [10,20,100000000000000000]
#
# x = random.choices(l,w,k=1).pop()
# print('complete')


class Q_graph:
    def __init__(self,q_graph_dict, alpha=1,gamma=1):
        self.q_graph = q_graph_dict
        self.current_node = random.choice(list(q_graph_dict.keys()))
        self.alpha = alpha
        self.gamma = gamma

    def optimize(self,iterations,supidity_rate=.1):
        for i in range(iterations):
            print(self.current_node)

            self.decide_neighbor(supidity_rate)

            print(self.current_node)
            print('***********************')
        print(self.q_graph)

    def update_q_values(self, current_node, new_node):
        old_q,reward = self.q_graph[current_node][new_node]
        neighbors = list(self.q_graph[new_node].keys())
        max_neighbor_q = max([self.q_graph[new_node][x][0] for x in neighbors], default=0)
        new_q = old_q + self.alpha*( reward +  self.gamma*max_neighbor_q - old_q)
        self.q_graph[current_node][new_node] = (new_q,reward)


    def decide_neighbor(self,stupidity_rate=.3):
        neighbors = list(self.q_graph[self.current_node].keys())
        if len(neighbors) == 0:
            self.current_node = random.choice(list(self.q_graph.keys()))
        else:
            logic = ['smart','stupid']
            logic_weights = [1-stupidity_rate, stupidity_rate]
            decision = random.choices(logic,logic_weights,k=1).pop()
            print(decision)
            if decision == 'stupid':
                new_neighbor = random.choice(neighbors)
            else:
                neighbor_qs = [self.q_graph[self.current_node][x][0] for x in neighbors]
                new_neighbor = random.choices(neighbors,neighbor_qs,k=1).pop()
            self.update_q_values(self.current_node,new_neighbor)
            self.current_node = new_neighbor





# class optimize:
#     def __init__(self,q_graph):





q_graph_dict = dict()
for i in range(11):
    if i == 0:
        q_graph_dict.update({i: {1: (1,0)}})
    elif i == 10:
        q_graph_dict.update({i: {}})
    elif i == 9:
        q_graph_dict[9] = {8:(1,0), 10: (1,1000000000)}
    else:
        q_graph_dict.update({i: {i-1: (1,0), i+1: (1,0)}})


q_graph = Q_graph(q_graph_dict)
q_graph.gamma = .1
q_graph.optimize(iterations=1000,supidity_rate=.1)
# for i in range(10):
#     print(q_graph.current_node)
#
#     q_graph.decide_neighbor()
#
#     print(q_graph.current_node)
#     print('***********************')


print('cpsdfa')
