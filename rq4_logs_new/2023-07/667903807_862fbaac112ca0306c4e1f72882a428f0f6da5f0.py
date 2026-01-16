def find_max_height(buildings, edges, current=0, points=[]):
    if len(edges) == 1:  # caso base, se tiver apenas uma aresta, acrescentamos a altura 0 na última posição
        points.append((edges[0], 0))
        return points

    i = edges[0]
    active = []
    for building in buildings:  # montando os edifícios válidos
        if building[0] <= i and building[2] > i:
            active.append(building)

    if not active:
        points.append((i, 0))  # se não houver edifícios ativos, acrescentamos a altura 0
        current = 0
    else:
        max_h = 0
        for building in active:  # montando a nova solução da skyline
            if building[1] > max_h:
                max_h = building[1]
        if max_h != current:
            current = max_h
            points.append((i, max_h))

    return find_max_height(buildings, edges[1:], current, points)

buildings = [(0, 8, 5), (2, 10, 9), (1, 4, 7), (11, 5, 15), (17, 11, 20), (19, 17, 22), (14, 3, 28), (25, 13, 30), (8, 6, 23)]

edges = []
for building in buildings:
    edges.extend([building[0], building[2]])
edges = sorted(edges)

result = find_max_height(buildings, edges)
print(result)