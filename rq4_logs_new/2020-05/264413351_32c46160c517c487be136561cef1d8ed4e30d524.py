''' This program returns the factorial of the number passes'''
from pyproblems.utility import is_int
def factorial_num(number):
    '''The function will return the factorial of the given number'''
    if not is_int(number):
        raise TypeError(f'unsupported format, {type(number)} is in the place of"integer"')
    if number < 0:
        raise ValueError("pass a positive integer")
    if number == 1 or 0:
        return 1
    result = 1
    if number > 1:
        result = number*factorial_num(number-1)
    return result