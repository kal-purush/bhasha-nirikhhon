'''This module has a function for amstrong number'''
from pyproblems.utility import is_int

def is_amstrong_number(number):
    '''This function returns True if the number is amstrong number,
    returns False otherwise'''
    if not is_int(number):
        raise TypeError(f"Unsupported type {type(number)}")

    num_str = str(number)
    num_digits = len(num_str)
    sum_of_digits = 0

    for digit in num_str:
        sum_of_digits += (int(digit) ** num_digits)

    return number == sum_of_digits