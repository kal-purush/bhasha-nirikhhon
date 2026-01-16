# Реализовать функцию, принимающую два числа (позиционные аргументы) и выполняющую их деление.
# Числа запрашивать у пользователя, предусмотреть обработку ситуации деления на ноль.

def my_div(arg_1, arg_2):
    """Возвращает результат деления arg_1 на arg_2"""
    try:
        return arg_1 / arg_2
    except ZeroDivisionError:
        print('На 0 делить нельзя')

print('my_div(2, 1) = ', my_div(2, 1))
print('my_div(1, 2) = ', my_div(1, 2))
print('my_div(-2, 1) = ', my_div(-2, 1))
print('my_div(-1, 2) = ', my_div(-1, 2))
print('my_div(0, 2) = ', my_div(0, 2))
print('my_div(2, 0) = ', my_div(2, 0))