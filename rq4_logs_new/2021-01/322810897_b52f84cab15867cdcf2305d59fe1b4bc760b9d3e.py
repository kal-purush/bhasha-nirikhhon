"""
1. Реализовать класс Matrix (матрица).
Обеспечить перегрузку конструктора класса (метод __init__()), который должен
принимать данные (список списков) для формирования матрицы.

Подсказка: матрица — система некоторых математических величин, расположенных
в виде прямоугольной схемы.
Примеры матриц вы найдете в методичке.

Следующий шаг — реализовать перегрузку метода __str__() для вывода матрицы
в привычном виде.
Далее реализовать перегрузку метода __add__() для реализации операции сложения
двух объектов класса Matrix (двух матриц). Результатом сложения должна быть
новая матрица.

Подсказка: сложение элементов матриц выполнять поэлементно — первый элемент
первой строки первой матрицы складываем с первым элементом первой строки
второй матрицы и т.д.
"""
import random as rnd


def bake(cols: int, rows: int):
    """
    Генератор матрицы указанных размеров.
    :return: список списков, наполненный псевдослучайными числами
    от 0 до 100
    """
    baked_matrix = []
    for col in range(abs(cols)):
        baked_row = []
        for row in range(abs(rows)):
            baked_row.append(rnd.randint(-100, 100))
        baked_matrix.append(baked_row)
    return baked_matrix


class Matrix:
    matrix_total = 0

    def __init__(self, matrix):
        self.m = {row: col for row, col in enumerate(matrix, start=1)}
        Matrix.matrix_total += 1

    def __str__(self):
        n = Matrix.matrix_total
        lines = [
            f'Размер матрицы {n}: {len(self.m.keys())} * {len(self.m.get(1))}',
        ]
        for key in self.m.keys():
            lines.append(''.join([f'{el: >4}' for el in self.m[key]]))
        return '\n'.join(lines)

    def __add__(self, other):
        baked_matrix = []
        l, r = self.m, other.m
        min_row = min(len(self.m.keys()), len(other.m.keys()))
        for i in range(min_row):
            row = list(map(lambda l_col, r_col: l_col + r_col, l[i+1], r[i+1]))
            baked_matrix.append(row)
        return Matrix(baked_matrix)


m1 = Matrix(bake(5, 7))
print(m1)

m2 = Matrix(bake(2, 9))
print(m2)

m_add = m1 + m2
print(f'{" Итоговая матрица ":*^40}')
print(m_add)