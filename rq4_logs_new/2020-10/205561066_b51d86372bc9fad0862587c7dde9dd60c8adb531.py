from dijkstra import construct_path
from pprint import pprint
import itertools

A = 'A'
B = 'B'
C = 'C'
D = 'D'
E = 'E'
F = 'F'
G = 'G'
INF = float("inf")


def construct_path(start, end, vertices, graph, results):
    node = end
    used_nodes = [node]
    while node != start and (len(used_nodes) < len(vertices) - 1):
        filtered_set = frozenset(vertices).difference(frozenset(used_nodes))
        filtered_results = {key: results[filtered_set][key] for key in results[filtered_set].keys()
                            & graph[node].keys()}

        node = min(filtered_results,
                   key=filtered_results.get)
        used_nodes.insert(0, node)

    return [A] + used_nodes + [A]

def held_karp(graph):
    results = {}
    # start can be any node
    start = A
    vertices = frozenset(graph.keys()) - frozenset([start])
    for j in vertices:
        if frozenset([start, j]) not in results:
            results[frozenset([start, j])] = {}
        results[frozenset([start, j])][j] = graph[start][j] if j in graph[start] else INF
    
    # print(results)

    for length in range(2, len(graph)):
        for set_ in itertools.combinations(vertices, length):
            set_1 = frozenset(set_).union(frozenset(start)) 
            for j in set_:
                # print(j)
                # print(set_)
                if set_1 not in results:
                    results[set_1] = {}
                results[set_1][j] = min([results[set_1.difference(frozenset(j))][k] + graph[k][j]
                                                for k in set_1.difference(frozenset([start, j]))
                                                if j in graph[k]
                                                ] or [INF])
    # pprint(results)
    all_vertices = vertices.union(frozenset([start]))
    costs = [[results[all_vertices][j] + graph[j][start], j] for j in vertices if start in graph[j]]
    min_cost = min([c[0] for c in costs])
    path_node = [c[1] for c in costs][[c[0] for c in costs].index(min_cost)]
    path = construct_path(start, path_node, all_vertices, graph, results)
    
    return path, min_cost

def main():
    graph1 = {A:{B: 1, C: 7, G: 2},
              B:{A: 1, C: 1, E: 6},
              C:{A: 7, B: 1, D: 9, E: 1},
              D:{C: 1, E: 1, F: 4, G: 8},
              E:{B: 6, C: 9, D: 1, F: 1},
              F:{E: 1, D: 4, G: 1},
              G:{A: 2, D: 8, F: 1},
              }

    graph2 = {A:{B: 1, C: 9, E: 10},
              B:{A: 1, C: 3},
              C:{A: 9, B: 3, D: 1, E: 4},
              D:{C: 1, E: 8, F: 7},
              E:{A: 10, C: 4, D: 8, F: 2, G: 7},
              F:{D: 7, E: 2, G: 3},
              G:{E: 7, F: 3},
              }

    pprint(graph1)
    path, min_distance = held_karp(graph1)
    print("Tour: ", path)
    print("Tour Distance: ", min_distance)

    pprint(graph2)
    path, min_distance = held_karp(graph2)
    print("Tour: ", path)
    print("Tour Distance: ", min_distance)

if __name__ == "__main__":
    main()