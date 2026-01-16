# Напишите программу, которая принимает на вход цифру, 
# обозначающую день недели, и проверяет, является ли этот день выходным.

i = int(input())
if 5 < i < 8:
    print('Weekend')
elif 0 < i < 5:
    print('Workday')
else:
    print('Its not a day of the week')

# Вариант 2
i = int(input())
if i == 1:
    print('Monday, Workday')
elif i == 2:
    print('Tuesday, Workday')
elif i == 3:
    print('Wednesday, Workday')
elif i == 4:
    print('Thursday, Workday')
elif i == 5:
    print('Friday, Workday')
elif i == 6:
    print('Saturday, Weekend')
elif i == 7:
    print('Sunday, Weekend')
elif 0 >= i > 7:
    print('Its not a day of the week')