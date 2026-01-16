import random
import queue
import math
import matplotlib.pyplot as plt



def create_graph(n, p):
    graph = {}
    for i in range(n):
        graph[i] = set()  # initialize adj set

    for i in range(n):
        adj_set_i = graph[i]
        # iterate over all other nodes, making sure each (i, j) pair i only compared once
        for j in range(i + 1, n):
            adj_set_j = graph[j]
            if random.random() <= p:  # with probability p assign an edge
                adj_set_i.add(j)
                adj_set_j.add(i)
    return graph

def create_fb_graph(file):

    with open(file) as f:
        content = f.readlines()

    content = [x.rstrip('\n') for x in content ]
    print('length', len(content))
    fb_graph = {}

    for elem in range(4039):
        if elem in fb_graph:
            continue
        else:
            fb_graph[elem] = set()

    for elem in content:
        node, connection = elem.split(' ')
        node = int(node)
        connection = int(connection)
        fb_graph[node].add(connection)
        fb_graph[connection].add(node)

    return fb_graph


def shortest_path(G, i, j):
    q = queue.Queue()
    visited = set()
    visited.add(i)
    distances = {}
    for j_candidate in G[i]:
        distances[j_candidate] = 1
        q.put(j_candidate)
        visited.add(j_candidate)  # are guaranteed to visit
    while not q.empty():
        j_candidate = q.get()
        if j_candidate == j:
            return distances[j_candidate]
        for j_candidate_neighbour in G[j_candidate]:
            if j_candidate_neighbour not in visited:
                visited.add(j_candidate_neighbour)
                q.put(j_candidate_neighbour)
                distances[j_candidate_neighbour] = distances[j_candidate] + 1
    return float("inf")


def avg_shortest_path(G, g_size, iterations, filename):
    sum_len = 0
    with open(filename, 'w') as file:
        for t in range(iterations):
            # generate a pair
            i = random.randint(0, g_size - 1)
            j = random.randint(0, g_size - 1)
            while i == j:  # in case they are the same
                j = random.randint(0, g_size - 1)
            length = shortest_path(G, i, j)
            if math.isinf(length):  # short circuit for efficiency
                return float(sum_len) / iterations
            sum_len += length
            file.write("({i}, {j}, {length})\n".format(i=i, j=j, length=length))
    return float(sum_len) / iterations


def varying_p_iterations(p_init, iterations, p_arr, p_increment, path_arr, file, n):
    for i in range(iterations):
        p_arr.append(p_init)
        G = create_graph(n, p_init)
        avg_short_path = avg_shortest_path(G, n, n, 'varying_p.txt')
        while math.isinf(avg_short_path):  # in case it is disconnected
            print('disconnected, recreating')
            G = create_graph(n, p_init)
            avg_short_path = avg_shortest_path(G, n, n, 'varying_p.txt')
        path_arr.append(avg_short_path)
        file.write("({p}, {avg_shortest_path})\n".format(p=p_init, avg_shortest_path=avg_short_path))
        p_init += p_increment


def varying_p(n):
    with open('tracing_p.txt', 'w') as file:
        p_init = 0.01
        p_increment = 0.01
        iterations = 4
        p_arr = []
        path_arr = []
        varying_p_iterations(p_init=p_init, iterations=iterations, p_arr=p_arr, p_increment=p_increment,
                             path_arr=path_arr, file=file, n=n)

        p_init = 0.05
        p_increment = 0.05
        iterations = 10
        p_arr = []
        path_arr = []
        varying_p_iterations(p_init=p_init, iterations=iterations, p_arr=p_arr, p_increment=p_increment,
                             path_arr=path_arr, file=file, n=n)
    plt.plot(p_arr, path_arr)
    plt.savefig('tracing_p.png')


if __name__ == '__main__':
    G = create_graph(1000, 0.1)
    print(avg_shortest_path(G, 1000, 1000, 'avg_shortest_path.txt'))
    varying_p(n=1000)
    fb_graph = create_fb_graph("/Users/joanneloh/Documents/Networks and Markets/facebook_combined.txt")
    print(avg_shortest_path(fb_graph, 4039, 1000, 'fb_shortest_path.txt'))
    FB = create_graph(4039, 0.01)
    print(avg_shortest_path(FB, 4039, 1000, 'fb_data.txt'))




