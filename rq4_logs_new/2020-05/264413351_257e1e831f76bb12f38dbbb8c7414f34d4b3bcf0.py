'''This program will evaluate the passed arguements and return the interest
amount throughout the term'''
from pyproblems.utility import is_int

def compound_interest(principle=1000, years=2, rate=7):
    '''This function will return the interest part of the principle
    compmpuonded throughout the term.
    Note: The interest is compounded annunaly, so the year must not be a float'''
    if not is_int(years):
        raise TypeError(f'Unsupported Format,{type(years)} passed in the placce of integer')
    for i in (principle, rate):
        if not isinstance(i, (int, float)) or isinstance(i, bool):
            raise TypeError('Unsupported Format')
    for i in (principle, years, rate):
        if i <= 0:
            raise ValueError('invalid entry')
    return round(((principle*(1+(rate/100))**years) - principle), 2)