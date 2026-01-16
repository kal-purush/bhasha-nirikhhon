#!/usr/bin/env python3
'''
    This function calculates the derivative of a polynomial
'''


def poly_derivative(poly):
    '''
    Returns the derivative of a polynomial
    '''
    if not (isinstance(poly, list) | len(poly) <= 1):
        return [0]
    return [poly[i] * i for i in range(1, len(poly))]