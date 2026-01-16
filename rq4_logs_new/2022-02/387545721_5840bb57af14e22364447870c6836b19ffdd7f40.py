#!/usr/bin/python3

"""Function to find perimeter of island in grid provided"""


def island_perimeter(grid):
    """Returns perimter of island in grid. 1 means land and 0 means water
    """

    width = len(grid[0])
    height = len(grid)
    x = 0
    y = 0
    perimeter = 0

    for x in range(height):
        for y in range(width):
            if grid[x][y] == 1:
                perimeter += 4
                if y > 0:
                    if grid[x][y - 1] == 1:
                        perimeter -= 1
                if y < (width - 1):
                    if grid[x][y + 1] == 1:
                        perimeter -= 1
                if x > 0:
                    if grid[x - 1][y] == 1:
                        perimeter -= 1
                if x < (height - 1):
                    if grid[x + 1][y] == 1:
                        perimeter -= 1
            y += 1
        x += 1
    return (perimeter)