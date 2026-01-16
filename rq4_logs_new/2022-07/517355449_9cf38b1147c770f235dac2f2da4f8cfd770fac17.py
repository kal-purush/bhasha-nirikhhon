'''
Given a Graph as a collection of edges' lists, return the shortest or smallest path between two nodes. Even if both DFS and BFS would solve
the problem, BFS is chosen because of how it works. DFS forces us to "look" in one direction on the graph traversal as further as possible,
and then start again, whereas BFS expands or fans out progressively in a "certain radius" from an origin node until the destination.

If by exploring with BFS in all directions I happen to find the target node, then by definition it must be the shortest path:
    - Because it's the first hit on a search area, which is an expanding radius, regarding of the path or neighbour chosen, they will all
    share the same radious size, or path weight.

Assumptions:
- If edge weight is not given, assume each edge has a weight of '1'

Observations:
- The algorithm should also store the distance from the starting point, in the queue that DFS uses.

Complexity:
    - Time Complexity: O(n), because once path is found, we don't have to keep traversing the graph.
    - Space Complexity: O(e), for the worst case scenario we need to store almost, if not, all of them.
'''

# Return the distance of the shortest path
def shortest_path(edges, src, dest):
    graph = convert_edges_list_to_graph(edges)
    dist = bfs_shortest_path(graph, src, dest)
    return dist

# DFS implementation that takes into account traversing distances
def bfs_shortest_path(graph, src, dest):
    visited = []
    queue = []
    visited.append(src)
    queue.append((src, 0)) # Use Python Tuple
    
    while queue:
        current = queue.pop()
        distance = current[1] + 1

        if current[0] == dest:
            return current[1]

        for neighbour in graph[current[0]]:
            if neighbour not in visited:
                visited.append(neighbour)
                queue.append((neighbour, distance))
    
    return -1 #if no path is found

# edge list is a list of lists
def convert_edges_list_to_graph(edge_list):
    graph = {}
    for edge in edge_list:
        source = edge[0]
        dest = edge[1]
        # If graph (Map of adjecency Lists) doesn't have entry for nodes' connection, then create for both.
        if source not in graph:
            graph[source] = []
        if dest not in graph:
            graph[dest] = []
        # As is undirected, add to both the connection of nodes.    
        graph[source].append(dest)
        graph[dest].append(source)
    return graph

if __name__ == "__main__":
    """
    Graphical Crude View:

        i --- j
        |     
        |     
        |     
        k --- l 
        |     |
        |     |
        m --- p

        o --- n
        

    """
    edges = [
        ["i", "j"],
        ["k", "i"],
        ["m", "k"],
        ["k", "l"],
        ["o", "n"],
        ["m", "p"],
        ["p", "l"]
    ]

    source = "i"
    destination = "l"
    shortest_path_distance = shortest_path(edges, source, destination)
    print(f"Shortest Path in Graph between {source} and {destination}: {shortest_path_distance}")
    no_path_distance = shortest_path(edges, 'i', 'n')
    print(f"Shortest Path in Graph between i and n: {no_path_distance}")