# Напишите программу, которая по заданному номеру четверти, показывает диапазон
# возможных координат точек в этой четверти (x и y).

def find_diapason(quarter):
    if 0 < quarter < 5:
        if quarter == 1:
            print('x>0, y>0')
        elif quarter == 2:
            print('x<0, y>0')
        elif quarter == 3:
            print('x<0, y<0')
        else:
            print('x>0, y<0')
    else:
        print('Wrong quarter. Input number from 1 to 4')


num = int(input('Input a quarter number: '))
find_diapason(num)