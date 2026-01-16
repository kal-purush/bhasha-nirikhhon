# Задача-1:
#
# Создать класс треугольник и реализовать в нем конструктор, методы для вычисления (не печати, нужен return) площади,
# периметра  и вывод значений сторон треугольника на экран.
# В конструкторе сделать проверку на возможность создания такого треугольника, если нет,
# то написать, что такой треугольник нельзя создать и сделать exit(1)

import math


class Triangle:

    def __init__(self):
        if not a + b > c and a + c > b and b + c > a:
            print("Такой треугольник нельзя создать")
            exit(1)
        self.a = a
        self.b = b
        self.c = c

    def print_sides(self, a, b, c):
        print(f'Стороны треугольника: {a} {b} {c}')

    def calc_quare(self, a, b, c):
        p = (a + b + c) / 2
        S = math.sqrt(p * (p-a) * (p-b) * (p-c))
        return S

    def calc_perimeter(self, a, b, c):
        P = a + b + c
        return P



