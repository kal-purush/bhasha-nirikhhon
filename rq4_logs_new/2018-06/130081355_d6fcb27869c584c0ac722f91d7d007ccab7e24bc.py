import numpy as np


class LUDecomposition:

    def __init__(self, matrix,vectors):
        self.matrix = matrix
        self.vectors = vectors
        self.validate()
        self.dim = self.matrix.shape[0]
        self.U = np.zeros((self.dim,self.dim))
        self.L = np.identity(self.dim)



    def validate(self):
        """
        We want to besure that the Matrix is Quadratic and that all vectors have the same dimension
        as the Matrix.
        :return:
        """
        matrix_shape = self.matrix.shape
        vectors_shape = self.vectors.shape

        assert matrix_shape[0] == matrix_shape[1]
        assert vectors_shape[1] == matrix_shape[1]

    def sum(self, limit=0, column=0, row=0):
        if limit < 0:
            return 0
        l = 0
        summ = 0.0
        while l <= limit:
            summ += self.L[row][l] * self.U[l][column]
            l += 1
        return summ

    def set_lower(self):
        self.L[1][0]= 1000

    def decomposition(self):
        k = 0
        while k <= self.dim - 1:
            j = 0
            while j <= k:
                self.U[j][k] = self.matrix[j][k] - self.sum(row=j, column=k, limit=j-1)
                j += 1

            j = k+1
            while j <= self.dim - 1:
                self.L[j][k] = (self.matrix[j][k] - self.sum(row=j, column=k, limit=j-1))/self.U[k][k]
                j += 1

            k += 1


matrix = np.array([[1.0,0.0,0.0],[0.0,1.0,0.0],[0.0,0.0,1.0]])

solving = LUDecomposition(matrix=matrix, vectors=np.array([[1, 1,1]]))
solving.decomposition()
print(solving.dim)
print(solving.matrix[0][1])
print("upper \n", solving.U)
print("lower \n", solving.L)