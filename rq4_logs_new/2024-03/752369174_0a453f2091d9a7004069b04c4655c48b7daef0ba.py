#GreedyBestFirstSearch
import heapq

graph = {'A' : {'B' : 5, 'C' : 3},
        'B' : {'D' : 8, 'E': 6},
        'C' : {'E' : 2, 'F' : 4},
        'D' : {'G' : 9},
        'E' : {'G' : 7},
        'F' : {},
        'G' : {}
}

heuristic = {
    'A' : 10,
    'B' : 8,
    'C' : 7,
    'D' : 6,
    'E' : 4,
    'F' : 3,
    'G' : 0
}

def gbfs(graph, start, goal, heuristic):
    visited = set()
    priority_queue = [(heuristic[start], start)]
    came_from = {}
    while priority_queue:
        current_cost, current_node = heapq.heappop(priority_queue)

        if current_node in visited:
            continue

        visited.add(current_node)

        if current_node == goal:
            path = [current_node]
            while current_node in came_from:
                current_node = came_from[current_node]
                path.insert(0, current_node)
            return path

        for neighbour, cost in graph[current_node].items():
            if neighbour not in visited:
                heapq.heappush(priority_queue, (heuristic[neighbour], neighbour))
                came_from[neighbour] = current_node
    return None

print(gbfs(graph, 'A', 'G', heuristic))