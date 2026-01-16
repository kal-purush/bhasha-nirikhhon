'''To find the sum of both leading and secondary diagonals'''
def diagonal_sum(matrix_list):
    '''This function takes in a valid list as matrix and returns the sum of both the diagonals '''
    if not isinstance(matrix_list, list):
        raise TypeError("Enter the matrix as a list of lists")
    for i, _ in enumerate(matrix_list):
        if not isinstance(matrix_list[i], list):
            raise TypeError("The elements in the list, should also be a list")
    if len(matrix_list) != len(matrix_list[0]):
        raise IndexError("Invalid Matrix Size, Enter a square matrix")
    for i, _ in enumerate(matrix_list):
        if not isinstance(matrix_list[i][i], int) or  isinstance(matrix_list[i][i], bool):
            raise TypeError(f"Invalid type of entry{type(matrix_list[i][i])}")
    leading_diag = 0 #Principal diagonal
    secondary_diag = 0 #secondary diagonal
    length = len(matrix_list)-1
    for i in range(length+1):
        leading_diag += matrix_list[i][i]
        secondary_diag += matrix_list[i][length -(i+1)]
    print('Sum of the diagonals are:')
    print(f'Principal diagonal = {leading_diag}')
    print(f'Secondary duagonal = {secondary_diag}')
    print(f'sum of both the diagonal elements are = {leading_diag+secondary_diag}')
    return leading_diag+secondary_diag