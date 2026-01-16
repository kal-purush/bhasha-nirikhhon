# 2. Напишите программу, которая на вход принимает 5 чисел и находит максимальное из них.
#     Примеры:
#     - 1, 4, 8, 7, 5 -> 8
#     - 78, 55, 36, 90, 2 -> 90


import random


def fill_list():
    any_list = []
    n = int(input('Enter number of elements: '))

    for i in range(n):
        any_list.append(random.randint(1, 50))

    return any_list


def find_max(any_list):
    max = any_list[0]
    for i in range(len(any_list)):
        if any_list[i] > max:
            max = any_list[i]
    return max


my_list = fill_list()
print(my_list)
print(find_max(my_list))

