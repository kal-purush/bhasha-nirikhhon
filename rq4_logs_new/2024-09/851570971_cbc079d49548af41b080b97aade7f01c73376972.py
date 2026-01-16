import numpy as np

save = True

if (save):       # Create and save two matrices
    matrix1 = np.array([[1, 2, 3], [4, 5, 6]])
    matrix2 = np.array([[7.0, 8, 9], [10, 11, 12]])

    np.savez('matrices.npz', matrix1=matrix1, matrix2=matrix2)
else            # Load and print the matrices from the .npz file
    loaded_data = np.load('matrices_new.npz')

    loaded_matrix1 = loaded_data['matrix1']
    loaded_matrix2 = loaded_data['matrix2']
    loaded_matrix3 = loaded_data['matrix3']

    print(loaded_matrix1)
    print(loaded_matrix2)
    print(loaded_matrix3)