from plate import Plate
from pprint import pprint


num_paths = 0


def check_exit(maze, index):
    column_top = 0
    column_bottom = len(maze) - 1
    row = len(maze[0]) - 1
    return index == (column_bottom, row) or index == (column_top, row)


def check_next(maze, i):
    global num_paths
    if check_exit(maze, i):
        maze[i[0]][i[1]].is_visited = True
        num_paths += 1
        return maze
    if len(maze) > 1:
        for j in range(0, len(maze)):
            for k in range(i[1] + 1, len(maze[0])):
                if maze[j][k].value == maze[i[0]][i[1]].value and \
                        (j, k) != (i[0] - 1, i[1] + 1) and \
                        (j, k) != (i[0] + 1, i[1] + 1) and \
                        (j, k) != (i[0], i[1] + 1):
                    maze = check_next(maze, (j, k))
        if i[0] - 1 >= 0 and i[1] + 1 < len(maze[0]) and maze[i[0]][i[1]].value == maze[i[0]-1][i[1]+1].value:
            maze = check_next(maze, (i[0] - 1, i[1] + 1))
        if i[1] + 1 < len(maze[0]) and i[0] + 1 < len(maze) and maze[i[0]][i[1]].value == maze[i[0]+1][i[1]+1].value:
            maze = check_next(maze, (i[0] + 1, i[1] + 1))
        if i[1] + 1 < len(maze[0]):
            maze = check_next(maze, (i[0], i[1] + 1))
        maze[i[0]][i[1]].is_visited = True
    else:
        num_paths += 1
        for k in range(i[1] + 1, len(maze[0])):
            print(i)
            if maze[0][k].value == maze[i[0]][i[1]].value and (0, k) != (i[0], i[1] + 1):
                maze = check_next(maze, (0, k))
    return maze


if __name__ == '__main__':
    # maze = [
    #     [Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2))],
    #     [Plate('c', (1, 0)), Plate('a', (1, 1)), Plate('b', (1, 2))],
    #     [Plate('d', (2, 0)), Plate('e', (2, 1)), Plate('f', (2, 2))]
    # ]
    maze = [
        [Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)), Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)), Plate('a', (0, 2))],
        [Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 2))],
        [Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 2))],
        [Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 2))],
        [Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 2))],
        [Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 0)), Plate('a', (0, 1)), Plate('a', (0, 2)),
         Plate('a', (0, 2))],
    ]
    pprint(check_next(maze, (0, 0)))
    pprint(check_next(maze, (1, 0)))
    pprint(check_next(maze, (2, 0)))
    pprint(check_next(maze, (3, 0)))
    pprint(check_next(maze, (4, 0)))
    pprint(check_next(maze, (5, 0)))

    print(num_paths)