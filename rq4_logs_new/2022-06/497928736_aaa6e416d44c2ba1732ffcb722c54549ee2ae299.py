#!/usr/bin/python3
''' Add integer module '''


def add_integer(a, b=98):
    ''' Function that add to numbers '''
    type_a, type_b = type(a), type(b)
    if type_a != int and type_a != float:
        raise TypeError('a must be an integer')
    if type_b != int and type_b != float:
        raise TypeError('b must be an integer')

    return int(a) + int(b)