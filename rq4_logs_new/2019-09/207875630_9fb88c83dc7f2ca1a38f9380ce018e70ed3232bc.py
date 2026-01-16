# 4.Определить, какое число в массиве встречается чаще всего.

import random


SIZE = 10
MIN_ITEM = 0
MAX_ITEM = 100
array = [random.randint(MIN_ITEM, MAX_ITEM) for _ in range(SIZE)]
print(array)

array_set = set(array)
frequency_max = 1
most_frequent = 0
for item in array_set:
    frequency_in = array.count(item)
    if frequency_in > frequency_max:
        frequency_max = frequency_in
        most_frequent = item
if frequency_max == 1:
    print('Все элементы входят по одному разу')
else:
    print(f'Число {most_frequent} встречается чаще всего')

