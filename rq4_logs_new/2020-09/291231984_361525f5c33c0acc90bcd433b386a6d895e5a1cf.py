#1. Создать список и заполнить его элементами различных типов данных. Реализовать скрипт проверки типа данных каждого элемента.
# Использовать функцию type() для проверки типа. Элементы списка можно не запрашивать у пользователя, а указать явно, в программе.

lst = ['Строка', 10000000, 0.16, False, [1, 2], {2, 3}, (3, 4), dict({1: 'd'})]

i = lst[0]
if type(i) is str:
    print(f"Тип {i} - действительно {type(i)}")
else:
    print(f"Ошибка, тип {i} д.б. {type(i)}")

i = lst[1]
if type(i) is int:
    print(f"Тип {i} - действительно {type(i)}")
else:
    print(f"Ошибка, тип {i} д.б. {type(i)}")

i = lst[2]
if type(i) is float:
    print(f"Тип {i} - действительно {type(i)}")
else:
    print(f"Ошибка, тип {i} д.б. {type(i)}")

i = lst[3]
if type(i) is bool:
    print(f"Тип {i} - действительно {type(i)}")
else:
    print(f"Ошибка, тип {i} д.б. {type(i)}")

i = lst[4]
if type(i) is list:
    print(f"Тип {i} - действительно {type(i)}")
else:
    print(f"Ошибка, тип {i} д.б. {type(i)}")

i = lst[5]
if type(i) is set:
    print(f"Тип {i} - действительно {type(i)}")
else:
    print(f"Ошибка, тип {i} д.б. {type(i)}")

i = lst[6]
if type(i) is tuple:
    print(f"Тип {i} - действительно {type(i)}")
else:
    print(f"Ошибка, тип {i} д.б. {type(i)}")

i = lst[7]
if type(i) is dict:
    print(f"Тип {i} - действительно {type(i)}")
else:
    print(f"Ошибка, тип {i} д.б. {type(i)}")