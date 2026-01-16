class Matrix:
    def __init__(self, matrix=[[1, 2, 3], [4, 5, 6], [7, 8, 9]]):
        self.matrix = matrix

    def __str__(self):
        return f'{self.matrix}'

    def __add__(self, other):
        res = []
        numb = []
        for i in range(len(self.matrix)):
            for j in range(len(self.matrix[0])):
                sum = other.matrix[i][j] + self.matrix[i][j]
                numb.append(sum)
                if len(numb) == len(self.matrix):
                    res.append(numb)
                    numb = []
        return Matrix(res)


mat_1 = Matrix()
mat_2 = Matrix()
print(mat_1 + mat_2)