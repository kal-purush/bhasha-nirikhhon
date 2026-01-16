def func(a, b):
    len_a = len(a)
    if len_a < 6:
        print('Пароль меньше 6ти символов!')
        b = 1
    x = 'password' in a.lower()
    if x == True:
        print('Пароль не должен содержать слово "password" в любом регистре!')
        b = 1
    if a.isalpha() == True:
        print('Пароль должен содержать хотя бы 1 цифру!')
        b = 1
    if a.isdigit() == True:
        print('Пароль не должен состоять из цифр!')
        b = 1
    if b == 1:
        return False
    elif b == 0:
        return True

print(func(a = input('Введите пароль:'), b=0))