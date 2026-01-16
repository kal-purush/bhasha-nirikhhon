'''
The Luhn Algorithm

Used to verify credit card numbers

From Wikipedia: http://en.wikipedia.org/wiki/Luhn_algorithm

1. From the rightmost digit, which is the check digit, moving left, double the value of every second digit; if product of this doubling operation is greater than 9 (e.g., 7 *
2 = 14), then sum the digits of the products (e.g., 10: 1 + 0 = 1, 14: 1 + 4 = 5).
2. Take the sum of all the digits.

The Luhn sum of a valid credit card number is a multiple of 10.
'''

# Non Recursive Approach
from re import M


def luhn(n):
    sum = 0
    while n!=0:
        n=n//10
        sum += n%10
        temp = 2*(n%10)
        if temp>9:
            sum += get_sum(temp)
        else:
            sum += temp
        n = n//10
    return sum

# Recursive Approach
def luhnr(n):
    if n<10:
        return n
    else:
        all_but_last , last = all_last(n)
        return luhn_sum_double(all_but_last)+last

def luhn_sum_double(n):
    all_but_last , last = all_last(n)
    luhn_digit = get_sum(2*last)
    if n<10:
        return luhn_digit
    else:
        return luhnr(all_but_last) + luhn_digit

# def luhn(n):
#     while n!=0:
#         n= n//10
#         print(n%10)
#         n = n//10
        
def all_last(n):
    return n//10, n%10

def get_sum(n):
    if n<10:
        return n
    else:
        all_but_last , last = all_last(n)
        return get_sum(all_but_last)+last

# def luhn(n):
#     sum = 0
#     while n!=0:
#         temp = (n%10)*2
#         while temp>9:
#             temp_sum = 0
#             while temp!=0:
                