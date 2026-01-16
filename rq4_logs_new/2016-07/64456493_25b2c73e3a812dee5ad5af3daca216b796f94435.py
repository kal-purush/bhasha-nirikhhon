# -*- encoding: utf-8 -*-

# TODO: pretty view
# None - empty cells
sudoku_example = [
    [None,    7,    6, None,    9,    5, None,    8, None],
    [   5,    2,    9, None, None,    8,    7,    3, None],
    [None, None, None,    7, None, None, None,    5,    9],
    [   7,    1,    4,    6,    3,    2, None,    9, None],
    [None, None, None, None, None, None, None, None, None],
    [None,    9, None,    5,    8,    1,    4,    2,    7],
    [   9,    8, None, None, None,    4, None, None, None],
    [None,    3,    7,    9, None, None,    8,    4,    5],
    [None,    6, None,    8,    2, None,    9,    1, None],
]

def print_sudoku(sudoku):
    print('\n'.join(', '.join(' ' if cell is None else str(cell) for cell in row) for row in sudoku))


#0 -> (0,0) (0,1) (0,2) (1,0) (1,1) (1,2)  (2, 0) (2, 1) (2, 2)
#1 -> (3,0) (3,1) (3,2) (4,0) (4,1) (4,2)  (5, 0) (5, 1) (5, 2)
def squads_cells(fields, squad):
    row = (squad // 3) * 3
    column = (squad % 3) * 3
    for i in range(3):
        for j in range(3):
            yield fields[row + i][column + j]

get_squad_by_coords = lambda i, j: i // 3 + j // 3

def get_possible_values(fields, i, j):
    if fields[i][j] is not None:
        return set()
    possible = set(range(1, 10))

    for _i in range(9):
        possible.discard(fields[_i][j])

    for _j in range(9):
        possible.discard(fields[i][_j])
        
    for cell in squads_cells(fields, get_squad_by_coords(i, j)):
        possible.discard(cell)

    return possible

class Sudoku:

    def __init__(self, fields):
        self.fields = fields
        
        self.possible_values = [[get_possible_values(fields, i, j) for j in range(9)] for i in range(9)]
        self.rows = [set(filter(None, row)) for row in fields]
        self.columns = [set(row[i] for row in fields if row[i] is not None) for i in range(9)]
        self.squads = [set(filter(None, squads_cells(i))) for i in range(9)]


if __name__ == '__main__':
    sudoku = Sudoku(sudoku_example)
    print_sudoku(sudoku_example)