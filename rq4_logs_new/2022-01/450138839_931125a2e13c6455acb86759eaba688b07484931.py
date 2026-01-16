#  Написать программу копирования массива
import random
def array (list):
    for i in range(0, 5):
        i = random.randint(1, 10)
        list.append(i)
    return list

def copy (list1, list2):
    list2 = list1
    return list2

list1 = []
list2 = []
print(array(list1))
print(copy(list1, list2))