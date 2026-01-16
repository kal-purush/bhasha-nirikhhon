#!/usr/bin/python
# -*- coding: utf-8 -*-
__author__ = "Sanchit"

def hourglass(n):
## Finding if the n is odd and setting up some variables
    if (n % 2 == 0):
        x = 2
        y = 1
        odd = False
    else:
        x = 1
        y = 2
        odd = True

    ## Get the top half of hourglass
    limit = n
    while n >= x:
        print ("*"*n).center(limit)
        n = n - 2

    ## setup n again for the bottom half of hourglass
    if odd:
     n = x + y
    else:
        n = x

    ## Get the bottom half of hourglass
    while n <= limit:
        print ("*"*n).center(limit)
        n = n + 2

print hourglass(6)
print hourglass(3)