import src  # My utility functions
from collections import defaultdict

STRINGS = src.read()


def build_map(strings):
    connections = defaultdict(set)
    large_caves = set()
    for string in strings:
        a, b = string.split('-')
        connections[a].add(b)
        connections[b].add(a)
        for cave in (a, b):
            if cave == cave.upper():
                large_caves.add(cave)
    return connections, large_caves


def find_paths(cave_map, large_caves):
    paths = 0
    queue = [('start', {'start'})]  # Each node consists of (current cave, {visited small caves})
    while queue:
        current, visited = queue.pop(-1)
        for neighbour in cave_map[current] - visited:
            if neighbour == 'end':
                paths += 1
            else:
                new_visited = visited if neighbour in large_caves else visited | {neighbour}
                queue.append((neighbour, new_visited))
    return paths


def find_paths_2(cave_map, large_caves):
    paths = 0
    queue = [('start', {'start'}, False)]  # Each node consists of (current, {visited smalls}, small repeated)
    while queue:
        current, visited, repeated = queue.pop(-1)
        for neighbour in cave_map[current]:
            if neighbour == 'end':
                paths += 1
            elif neighbour != 'start' and (not repeated or neighbour not in visited):
                new_repeated = repeated or bool(neighbour in visited)
                new_visited = visited if neighbour in large_caves else visited | {neighbour}
                queue.append((neighbour, new_visited, new_repeated))
    return paths


def main(strings=STRINGS):
    print("Part One:")
    cave_map, large_caves = build_map(strings)
    ans1 = find_paths(cave_map, large_caves)  # 3230
    print(f"There are {ans1} viable paths through the cave system.")

    print("\nPart Two:")
    ans2 = find_paths_2(cave_map, large_caves)  # 83475
    print(f"There are now {ans2} viable paths.")
    src.clip(ans2)


if __name__ == '__main__':
    main()