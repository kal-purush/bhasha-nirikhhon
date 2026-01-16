import math
import time
import pprint as pp

example_sudoku = [[5, 3, 0, 0, 7, 0, 0, 0, 0],
                  [6, 0, 0, 1, 9, 5, 0, 0, 0],
                  [0, 9, 8, 0, 0, 0, 0, 6, 0],
                  [8, 0, 0, 0, 6, 0, 0, 0, 3],
                  [4, 0, 0, 8, 0, 3, 0, 0, 1],
                  [7, 0, 0, 0, 2, 0, 0, 0, 6],
                  [0, 6, 0, 0, 0, 0, 2, 8, 0],
                  [0, 0, 0, 4, 1, 9, 0, 0, 5],
                  [0, 0, 0, 0, 8, 0, 0, 7, 9]]


def remove_zeros(list_of_values):
    return [x for x in list_of_values if x != 0]


def check_nine_unique(list_of_values, list_type):
    without_zeros = remove_zeros(list_of_values)
    if len(set(without_zeros)) != len(without_zeros):
        raise Exception("Error with " + list_type + ": " + str(list_of_values))


def convert_to_columns(grid):
    return [[line[i] for line in grid] for i in range(9)]


def convert_to_squares(grid):
    squares = []
    for x in range(3):
        for y in range(3):
            x_values = range(3 * x, 3 * x + 3)
            y_values = range(3 * y, 3 * y + 3)
            square = [grid[i][j] for i in x_values for j in y_values]
            squares.append(square)
    return squares


def check_grid_valid(grid):
    # checks grid format is valid
    for row in grid:
        if len(row) != 9:
            raise Exception("Line length incorrect " + str(row))
    if len(grid) != 9:
        raise Exception("Number of lines incorrect")

    # check grid only contains values 0 - 9
    for row in grid:
        for value in row:
            if value not in range(10):
                raise Exception("Invalid value, sudoku must only contain numbers 0-9")

    # check lines are valid
    for row in grid:
        check_nine_unique(row, "row")

    # check columns are valid
    for column in convert_to_columns(grid):
        check_nine_unique(column, "column")

    # check squares are valid
    for square in convert_to_squares(grid):
        check_nine_unique(square, "square")

    print("Sudoku puzzle is valid.")


def convert_grid_of_possibilities(grid):
    grid_of_possibilities = []

    for row in grid:
        new_row = [[value] if value != 0 else list(set(range(1, 10))) for value in row]
        grid_of_possibilities.append(new_row)

    return grid_of_possibilities


def convert_to_grid(grid_of_possibilities):
    grid = []

    for row in grid_of_possibilities:
        new_row = [0 if len(value) > 1 else value[0] for value in row]
        grid.append(new_row)

    return grid


def solve_sudoku(grid, grid_of_possibilities):

    columns = convert_to_columns(grid)
    squares = convert_to_squares(grid)

    for row in (range(9)):
        for column in range(9):

            # remove possibilities by row
            if len(grid_of_possibilities[row][column]) > 1:
                grid_of_possibilities[row][column] = list(set(grid_of_possibilities[row][column])
                                                          - set(grid[row]))
            # remove possibilities by column
            if len(grid_of_possibilities[row][column]) > 1:
                grid_of_possibilities[row][column] = list(set(grid_of_possibilities[row][column])
                                                          - set(remove_zeros(columns[column])))
            # remove possibilities by square
            if len(grid_of_possibilities[row][column]) > 1:
                square_index = math.floor(row / 3) * 3 + math.floor(column / 3)
                grid_of_possibilities[row][column] = list(set(grid_of_possibilities[row][column])
                                                          - set(remove_zeros(squares[square_index])))

    new_grid = convert_to_grid(grid_of_possibilities)
    print("\nNEW GRID:")
    pp.pprint(new_grid)

    return new_grid, grid_of_possibilities


def check_if_incomplete(grid):
    grid_values = [value for row in grid for value in row]
    if 0 in grid_values:
        return True
    else:
        return False


def main(sudoku_puzzle):

    start_time = time.time()

    check_grid_valid(sudoku_puzzle)

    sudoku_unsolved = True
    grid_of_possibilities = convert_grid_of_possibilities(sudoku_puzzle)
    iteration = 1

    while sudoku_unsolved:
        sudoku_puzzle, grid_of_possibilities = solve_sudoku(sudoku_puzzle, grid_of_possibilities)
        print("Iteration", iteration, "complete")

        sudoku_unsolved = check_if_incomplete(sudoku_puzzle)
        iteration += 1

        if iteration == 100:
            raise Exception("This sudoku puzzle seems unsolvable. Are you sure you inputted the numbers in correctly?")

    print("\nSudoku puzzle has been solved. Time taken:", (time.time() - start_time) / 1000, "ms")


main(example_sudoku)