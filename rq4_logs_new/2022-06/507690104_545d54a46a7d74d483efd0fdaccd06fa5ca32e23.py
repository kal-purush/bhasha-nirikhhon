#!/usr/bin/python3
"""Module to find the max integer in a list
"""


def max_integer(list=[]):
    """Function to find and return the max integer in a list of integers
        If the list is empty, the function returns None
    """
    list1 = list
    list2 = []
    
    if type(list1) is not type(list2):
        raise TypeError("list must be of type list")
      
    for x in list1:
        if type(x) is not int:
            raise TypeError("element of list must be an integer")  
    
    
    if len(list1) == 0:
        return None
    result = list1[0]
    i = 1
    while i < len(list1):
        if list1[i] > result:
            result = list1[i]
        i += 1
    return result