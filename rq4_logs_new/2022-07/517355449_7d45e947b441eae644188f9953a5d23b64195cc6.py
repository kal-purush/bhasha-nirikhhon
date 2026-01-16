from queue import Queue

''' Picks a starting node in a Graph, moves in ALL adjacent nodes' direction until is no longer possible (depends on directed or undirected graph). 
    At every visited node, adds the "neighbour" nodes to a queue, which guarantees that as they are added they'll be visited, before jumping to a second "level" of traverse, 
    because in a queue the first one added is the first one visited, or, they are visited in the order they are added.
    
    If's a "spanning" type of algorithm, using the queue as key.

    Important 1: It also keeps track of nodes already visited, to avoid visiting them again if another traversing iteration already went there, from a different node.
    
    KEY_TO_REMEMBER: Uses a QUEUE structure.
'''

def depth_first_traverse(graph, start_node):
    # 0 for queue size means infinite size
    queue = Queue(0)
    queue.put(start_node)

    while queue.qsize() > 0:
        current = queue.get()
        print(current)

        for neighbour in graph[current]:
            queue.put(neighbour)


def run_example():
    """
    Graphical Crude View:

        A --- C
        |     |
        |     |
        B     E
        |
        |
        D --- F

    """
    
    # Graph represented as Python dick (Map in Java) with adjacency list for a Node's neighbours
    graph = {
        "a": ['c', 'b'],
        "b": ['d'],
        "c": ['e'],
        "d": ['f'],
        "e": [],
        "f": []
    }

    depth_first_traverse(graph, "a")

def main():
    run_example()

if __name__ == "__main__":
    main()