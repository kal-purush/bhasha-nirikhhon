# Задание-1
# Куб состоит из n^3 прозрачных и непрозрачных элементарных кубиков.
# Имеется ли хотя бы один просвет по каждому из трех измерений?
# Если это так, вывести координаты каждого просвета. Куб задается трехмерной матрицей из 0 и 1.


import random
import pprint

cube = [[[random.randint(0, 1) for k in range(3)] for j in range(3)] for i in range(3)]

pprint.pprint(cube)

empty_row = [0] * 3

for matr in cube:
    for row in matr:
        print("Yes" if row == empty_row else "No")


for matr in cube:
    for i in range(3):
        buf = []
        for j in range(3):
            buf.append(matr[i][j])
        print("Yes" if buf == empty_row else "No")



for i in range(3):
    for j in range(3):
        buf = []
        for height in range(3):
            buf.append(cube[height][i][j])
        print("Yes" if buf == empty_row else "No")

