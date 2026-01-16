import collections

def search_maze(start, end, path, visited):
    path.append(start)
    if(start==end):
        return True, path
    for c in maze[start]:
        if c not in visited:
            visited.append(c)
            return search_maze(c, end, path, visited)
    return False, None

def connect(a, L):
    for b in L:
        maze[Coordinate(a.x, a.y)].append(Coordinate(b.x,b.y))
        maze[Coordinate(b.x, b.y)].append(Coordinate(a.x,a.y))


maze = collections.defaultdict(list)
Coordinate = collections.namedtuple('Coordinate', ('x', 'y'))

connect(Coordinate(0, 2), [Coordinate(0,3), Coordinate(1, 2)])
connect(Coordinate(1, 1), [Coordinate(1, 2), Coordinate(2, 1)])
connect(Coordinate(2, 1), [Coordinate(3, 1)])
connect(Coordinate(2, 3), [Coordinate(3, 3)])
connect(Coordinate(3, 0), [Coordinate(3, 1)])
connect(Coordinate(3, 1), [Coordinate(3, 2)])
connect(Coordinate(3, 2), [Coordinate(3, 3)])

result = search_maze(Coordinate(3, 0), Coordinate(0, 3), [], [])
if result[0]:
    print(result[1])