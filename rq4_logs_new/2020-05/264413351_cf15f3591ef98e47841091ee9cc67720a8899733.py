"""4. Given a number, return a list containing individual digits.
Ex: 1234 should be transformed as [1, 2, 3, 4]"""

from pyproblems.utility import is_int

def number_to_list_digits(number):
    '''This function returns individual digits'''
    if not is_int(number):
        raise ValueError(f"Invalid Inputs {type(number)}, {number}")

    number_str = str(number)
    if number_str.isnumeric():
        return [int(char) for char in number_str]
    raise ValueError(f"Invalid Inputs {type(number)}, {number}")