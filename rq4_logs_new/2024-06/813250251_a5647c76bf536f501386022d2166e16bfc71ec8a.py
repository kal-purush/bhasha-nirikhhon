# Check loops and isolations function


import maze_constants as c


def check_maze(rows, cols, right_walls, down_walls) -> str:
    '''Check loops and isolations function

    Return:
    - visited points list that can be reached from point (0, 0)
    - empty list if loops detected
    '''
    start = (0, 0)
    visited_points = [start]
    parents = {start: None}
    for point in visited_points:
        # up direction:
        next_point = (point[c.ROW] - 1, point[c.COL])
        if down_walls[next_point[c.ROW]][next_point[c.COL]] == c.PASS:
            if next_point in visited_points and \
               next_point != parents.get(point, None):
                return c.LOOPS
            elif next_point not in visited_points and next_point[c.ROW] >= 0:
                visited_points.append(next_point)
                parents[next_point] = point

        # right direction
        next_point = (point[c.ROW], point[c.COL] + 1)
        if right_walls[point[c.ROW]][point[c.COL]] == c.PASS:
            if next_point in visited_points and \
               next_point != parents.get(point, None):
                return c.LOOPS
            elif next_point not in visited_points and next_point[c.COL] < cols:
                visited_points.append(next_point)
                parents[next_point] = point

        # down direction:
        next_point = (point[c.ROW] + 1, point[c.COL])
        if down_walls[point[c.ROW]][point[c.COL]] == c.PASS:
            if next_point in visited_points and \
               next_point != parents.get(point, None):
                return c.LOOPS
            elif next_point not in visited_points and next_point[c.ROW] < rows:
                visited_points.append(next_point)
                parents[next_point] = point

        # left direction
        next_point = (point[c.ROW], point[c.COL] - 1)
        if right_walls[next_point[c.ROW]][next_point[c.COL]] == c.PASS:
            if next_point in visited_points and \
               next_point != parents.get(point, None):
                return c.LOOPS
            elif next_point not in visited_points and next_point[c.COL] >= 0:
                visited_points.append(next_point)
                parents[next_point] = point

    return c.PERFECT if len(visited_points) == rows * cols else c.ISOLATIONS


if __name__ == '__main__':
    print(check_maze.__doc__)