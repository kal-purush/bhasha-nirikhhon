import numpy as np
import random


def mas():
    """Создание и вывод массива размером 3х3
    
    Не принимает никаких переменных

    Выводит массив размером 3х3 со случайными числами
    """
    a = np.empty((3,3), dtype="float32")
    for i in range(0,2):
        for j in (0,2):
            a[i][j] = random.random()

    a_trans =  a.transpose()
    print(a)
    print(a_trans)

def delenie(a,b):
    """Деление двух чисел
    
    Принимает:
        a - первое число
        b - второе число
    Возвращает:
        Результат деления a на b
    """
    print(a / b)