'''This program is to find out the given two numbers are amicable number or not'''
from pyproblems.utility import is_int
def amicable_numbers(num1, num2):
    '''This function returns (Amicable number) if the numbers are amicalbe number,
    returns (Not Amicable) otherwise'''
    for i in (num1, num2):
        if not is_int(i):
            raise TypeError("unsupported format")
    for i in (num1, num2):
        if i < 1:
            raise ValueError("Enter a number greater than 1")
    sum1 = 0
    sum2 = 0
    for i in range(1, num1):
        if num1%i == 0:
            sum1 += i
    for j in range(1, num2):
        if num2%j == 0:
            sum2 += j
    return sum1 == num2 and sum2 == num1
 