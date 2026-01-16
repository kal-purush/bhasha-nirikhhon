# Напишите программу, которая принимает на вход координаты точки (X и Y), причём X ≠ 0 и Y ≠ 0 и
# выдаёт номер четверти плоскости, в которой находится эта точка (или на какой оси она находится).
# Пример: - x=34; y=-30 -> 4; - x=2; y=4-> 1; - x=-34; y=-30 -> 3

def find_quarter(x, y):
    if x != 0 and y != 0:
        if x > 0 and y > 0:
            print('I')
        elif x < 0 and y > 0:
            print('II')
        elif x < 0 and y < 0:
            print('III')
        else:
            print('IV')
    else:
        print("X or Y can't be equal to 0")


find_quarter(34, -30)
find_quarter(2, 4)
find_quarter(-34, -30)
find_quarter(5, 0)
