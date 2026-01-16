# Определить, какое число в массиве встречается чаще всего.

import random
import cProfile
# вариант 1


# def frequency(min_item, size):
#     array = [random.randint(min_item, size // 1.5) for _ in range(size)]
#     num = array[0]
#     frequency = 1
#     for i in range(len(array)):
#         spam = 1
#         for j in range(i + 1, len(array)):
#             if array[i] == array[j]:
#                 spam += 1
#         if spam > frequency:
#             frequency = spam
#             num = array[i]
#     return frequency, num

# "task_1.frequency(0, 10)"
# 100 loops, best of 5: 41.8 usec per loop
#  "task_1.frequency(0, 20)"
# 100 loops, best of 5: 92.6 usec per loop
# "task_1.frequency(0, 30)"
# 100 loops, best of 5: 160 usec per loop


#cProfile.run('frequency(0, 10)')


# вариант 2

# def frequency(min_item, size):
#     array = [random.randint(min_item, size // 1.5) for _ in range(size)]
#     counter = {}
#     frequency = 1
#     num = None
#     for item in array:
#         if item in counter:
#             counter[item] += 1
#         else:
#             counter[item] = 1
#
#         if counter[item] > frequency:
#             frequency = counter[item]
#             num = item
#     return frequency, num

# "task_1.frequency(0, 10)"
# 100 loops, best of 5: 31.9 usec per loop
# "task_1.frequency(0, 20)"
# 100 loops, best of 5: 63.6 usec per loop
# "task_1.frequency(0, 30)"
# 100 loops, best of 5: 93.9 usec per loop


# вариант 3

def frequency(min_item, size):
    array = [random.randint(min_item, size // 1.5) for _ in range(size)]
    array_set = set(array)
    frequency_max = 1
    most_frequent = 0
    for item in array_set:
        frequency_in = array.count(item)
        if frequency_in > frequency_max:
            frequency_max = frequency_in
            most_frequent = item
    return most_frequent

# "task_1.frequency(0, 10)"
# 100 loops, best of 5: 36.9 usec per loop
# "task_1.frequency(0, 20)"
# 100 loops, best of 5: 67.8 usec per loop
# "task_1.frequency(0, 30)"
# 100 loops, best of 5: 100 usec per loop

#cProfile.run('frequency(0, 20)')

# Все три варианта показали линейную сложность O(n)
# Тест cProfile не выявил слабых мест