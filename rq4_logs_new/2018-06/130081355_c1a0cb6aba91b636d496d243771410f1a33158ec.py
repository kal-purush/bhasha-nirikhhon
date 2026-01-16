import numpy as np
import sys
sys.path.append('../')

from part_1.jacoby import Jacoby


def build_matrix(dim=None, periodic=False):
    if not dim:
        raise ValueError("Please enter a Dimension for the matrix")
    dim_sq = dim **2
    last_row = dim_sq -dim
    matrix = 4*np.identity(dim_sq, dtype=float) # initialize j==k with -4

    if periodic:
        j = 0
        k = 0
        while j < dim_sq:
            while k < dim_sq:
                if k==j:
                    k += 1
                    continue
                if k < dim:
                    # left border of grid
                    pass
                elif not k % dim:
                    # top border of grid
                    pass
                elif not k+1 % dim:
                    # bottom of grid
                    pass
                elif k > last_row:
                    # right border of grid
                    pass
                else:
                    # inbetween
                    pass


                k += 1

            j += 1

    return matrix


matrix_to_solve = build_matrix(4)

print(matrix_to_solve)