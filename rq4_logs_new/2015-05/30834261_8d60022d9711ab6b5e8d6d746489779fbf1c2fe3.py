from functools import reduce

def largest_product_sequence(grid, sequence_length):
    """ given a grid and sequence length returns the sequence
    with the maximum product

    args:
      grid: the 2d array to sequence over.
      sequence_length: the length of the sequence for calculating
                       the product
    """

    def product(seq):
        return reduce(lambda x, y: x * y, seq)

    return max(product(seq) for seq in sequences(grid, sequence_length))


def sequences(grid, sequence_length):
    """ Geneartor that returns all possible permutations of grid."""
    col_length = len(grid)
    row_length = len(grid[0])

    def upward_seq_p(x, y):
        return y - (sequence_length - 1) >= 0

    def forward_seq_p(x, y):
        return x + sequence_length < row_length

    def forward_seq(x, y):
        coords = []
        for index in range(sequence_length):
            coords.append(grid[y][x+index])
        return coords

    def downward_seq_p(x, y):
        return y + sequence_length < col_length

    def downward_seq(x, y):
        coords = []
        for index in range(sequence_length):
            coords.append(grid[y+index][x])
        return coords

    def up_diag_p(x, y):
        return forward_seq_p(x, y) and upward_seq_p(x, y)

    def up_diag(x, y):
        coords = []
        for index in range(sequence_length):
            coords.append(grid[y-index][x+index])
        return coords

    def down_diag_p(x, y):
        return forward_seq_p(x, y) and downward_seq_p(x, y)

    def down_diag(x, y):
        coords = []
        for index in range(sequence_length):
            coords.append(grid[y+index][x+index])
        return coords

    for y in range(col_length):
        for x in range(row_length):
            if forward_seq_p(x, y):
                yield forward_seq(x, y)
            if downward_seq_p(x, y):
                yield downward_seq(x, y)
            if up_diag_p(x, y):
                yield up_diag(x, y)
            if down_diag_p(x, y):
                yield down_diag(x, y)
