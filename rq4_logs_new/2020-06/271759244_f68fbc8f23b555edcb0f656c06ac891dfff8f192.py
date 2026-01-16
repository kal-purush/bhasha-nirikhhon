
print('task1')
def my_func(var1, var2):
    var1 = float(input('Введите первое число '))
    var2 = float(input('Введите второе число '))
    result = var1 / var2
    return result

i = my_func(10, 5)
print(i)

print('task2')
def my_func2():
    name = input('Введите ваше имя: ')
    surname = input('Введите вашу фамилию: ')
    year = str(input('Введите ваш год рождения: '))
    city = input('Введите ваш город проживания: ')
    email = input('Введите ваш email: ')
    tel = str(input('Введите ваш номер телефона: '))
    result = name + ' ' + surname + ' ' + year + ' ' + city + ' ' + email + ' ' + tel
    return result
print(my_func2())

print('task3')
def my_func3(var1, var2, var3):
    a = [var1, var2, var3]
    a.sort()
    b = a[1] + a [2]
    return b
print(my_func3(40, 10, 50))

print('task4')
def my_func4(x, y):
    x = int(x)
    y = int(-y)
    result = x ** y
    float(result)
    return result
print(my_func4(16, 2))

print('task5')
# Не получилось сделать, изучил ваше решение.
def insert_sum(*args):
    result = 0
    exit_flag = False
    for itm in args:
        try:
            result += float(itm) if itm else 0
        except ValueError as e:
            if itm == 'q':
                exit_flag = not exit_flag
                break
    return result, exit_flag

user_sum = 0
user_exit = False
while not user_exit:
    user_point = input('Введите числа через пробел\n').split(' ')
    result_summ, user_exit = insert_sum(*user_point)
    user_sum += result_summ
    print(f'сумма: {user_sum}')




print('task6')
def int_func(var1):
    var1 = str(var1)
    return var1.title()
print(int_func('freedoM'))

my_str = 'room apple home friend'
print(int_func(my_str))











