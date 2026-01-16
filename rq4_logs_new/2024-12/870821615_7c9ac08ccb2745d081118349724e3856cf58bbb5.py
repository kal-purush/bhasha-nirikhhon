#!/usr/bin/python3
"""
Calculates the perimeter of an island represented by a grid of land and water.
"""


def island_perimeter(grid):
    """
    Calculates the perimeter of an island represented by a grid of
    land and water.

    Args:
        grid (List[List[int]]): A 2D grid where 1 represents land and
            0 represents water. The grid is surrounded by water,
            and there is exactly one island or none.

    Returns:
        int: The perimeter of the island. If no land is present, returns 0.

    Example:
        grid = [
            [0, 0, 0, 0, 0, 0],
            [0, 1, 0, 0, 0, 0],
            [0, 1, 0, 0, 0, 0],
            [0, 1, 1, 1, 0, 0],
            [0, 0, 0, 0, 0, 0]
        ]
        print(island_perimeter(grid))  # Output: 12
    """
    perimeter = 0

    # Traverse each cell in the grid
    for i in range(len(grid)):
        for j in range(len(grid[i])):
            # Check if the cell is land (1)
            if grid[i][j] == 1:
                # Check each of the four directions (up, down, left, right)
                if i == 0 or grid[i - 1][j] == 0:  # up
                    perimeter += 1
                if i == len(grid) - 1 or grid[i + 1][j] == 0:  # down
                    perimeter += 1
                if j == 0 or grid[i][j - 1] == 0:  # left
                    perimeter += 1
                if j == len(grid[i]) - 1 or grid[i][j + 1] == 0:  # right
                    perimeter += 1

    return perimeter