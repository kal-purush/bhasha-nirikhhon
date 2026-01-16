"""
Модуль с декораторами
"""
import time

#1
def decor_before_after(func):
    """Декоратор добавляет 'Before' и 'After' перед и после функции"""
    def wrapper(*args):
        print('Before')
        func(*args)
        print('After')
        return 

    return wrapper


#2
def plus_one(func):
    """Декоратор прибавляет единицу к аргументу функции"""
    def wrapper(*args):
        s = func(*args)
        result = s + 1
        return print(result)
    return wrapper


#3
def all_letters_capital(func):
    """Функция делает все буквы заглавными"""
    def wrapper(input_text):
        result = input_text.upper()
        return result
    return wrapper


#4
def func_name(func):
    """Функция выводит имя оборачиваемой функции"""
    def wrapper():
        return func.__name__
    return wrapper


#5
def change(func):
    """Функция меняет порядок принимаемых неименнованых аргументов в обратном порядке"""
    def wrapper(*args):
        sgra = list(args)
        sgra = sgra[::-1]
        s = func(*sgra)
        return s
    return wrapper


#6
def run_timer(func):
    """Функция добавляет к результату подаваемой функции ещё и время её выполнения"""
    def wrapper(*args):
        start_time = time.time()
        func(*args)
        end_time = time.time()
        run_time = end_time - start_time
        return print(f'Время выполнения вычислений: {run_time}сек.')
    return wrapper

