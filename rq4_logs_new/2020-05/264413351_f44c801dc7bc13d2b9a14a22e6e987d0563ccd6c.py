'''This module has some functions related to Pythagorean Triples'''
from pyproblems.utility import is_int

def is_pythagorean_triple(triple):
    '''This function returns True is the passed element is a pythagorean triple,
    returns False otherwise'''
    if isinstance(triple, tuple) and len(triple) == 3:
        if is_int(triple[0]) and is_int(triple[1]) and is_int(triple[2]):
            numbers = list(triple)
            numbers.sort()
            return (numbers[0] ** 2 + numbers[1] ** 2) == numbers[2] ** 2

        raise TypeError("Unsupported Type")

    raise TypeError(f"Unsupported Type {type(triple)}")