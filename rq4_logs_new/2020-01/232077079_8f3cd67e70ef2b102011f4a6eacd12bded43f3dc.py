def heuristic(h1, h2):
    (x1, y1) = h1
    (x2, y2) = h2
    return abs(x1 - x2) + abs(y1 - y2)

def search(graph, begin, end):
    border = PriorityQueue()
    border,put(begin, 0)
    cameFrom = {}
    costUpdate = {}
    cameFrom[begin] = None
    costUpdate[begin] = 0

    while not border.empty():
        current = border.get()

        if current == end:
            break

        for new in graph.edges(current):
            newCost = costUpdate[current] + graph.cost(current, new)
            if new not in costUpdate or newCost < costUpdate[new]:
                costUpdate[new] = newCost
                lead = newCost + heuristic(end, new)
                border.put(new, lead)
                cameFrom[new] = current
    return cameFrom, costUpdate