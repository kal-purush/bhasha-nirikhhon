'''This program will return the emi payable for the loan borrowed
over a period of time'''
from pyproblems.utility import is_int
from pyproblems.utility import is_float

def emi_calc(principle: "Money borrowed" = 100000, interest: "interest rate per year" = 10,
             years: "Term of the loan" = 3):
    '''The function will returm the Equated Monthly installment amount
    for the borrowed principle over a certain period at a certain rate of interest'''
    for i in (principle, years):
        if not is_int(i):
            raise TypeError(f'Unsupported format,{type(i)} in the place of "int"')
    if not is_float(interest) and not is_int(interest):
        raise TypeError(f'Unsupported format,{type(interest)} in the place of "int" or "float"')
    for i in (principle, interest, years):
        if i < 1:
            raise ValueError("Invalid Entry")

    monthly_int = interest / (12*100)

    months = years * 12

    present_value = 1/(1 + monthly_int)

    v_n = present_value ** months

    annuity = (1 - v_n) / monthly_int

    return round(principle/annuity)