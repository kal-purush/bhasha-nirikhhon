# Реализовать класс Matrix (матрица). Обеспечить перегрузку конструктора класса (метод __init__()), который должен принимать данные (список списков) для формирования матрицы.
# Подсказка: матрица — система некоторых математических величин, расположенных в виде прямоугольной схемы.
# Следующий шаг — реализовать перегрузку метода __str__() для вывода матрицы в привычном виде.
# Далее реализовать перегрузку метода __add__() для реализации операции сложения двух объектов класса Matrix (двух матриц). Результатом сложения должна быть новая матрица.
# Подсказка: сложение элементов матриц выполнять поэлементно — первый элемент первой строки первой матрицы складываем с первым элементом первой строки второй матрицы и т.д.

class Matrix:
    def __init__(self, matrix):
        self.matrix = matrix

    def __str__(self):
        res = ''
        for i in list(self.matrix):
             res += ' '.join(map(str, list(i))) + '\n'
        return res

    # матрицы д.б. одинаковой размерности
    def __add__(self, other):
        res = []
        for i in range(len(list(self.matrix))):
            lst = []
            for j in range(len(list(self.matrix[i]))):
                lst.append(float(list(list(self.matrix)[i])[j]) + float(list(list(other.matrix)[i])[j]))
            res.append(lst)
        return Matrix(res)


matrix1 = Matrix([[1, 2], [3, 4], [5, 6]])
matrix2 = Matrix([[11, 12], [13, 14], [15, 16]])

print(matrix1)
print(matrix2)
print(matrix1 + matrix2)
