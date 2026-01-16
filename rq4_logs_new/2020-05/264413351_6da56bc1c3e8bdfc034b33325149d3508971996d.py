'''This program seperates odd and even number from the list'''
from pyproblems.utility import is_int
def sep_odd_eve(list_int):
    '''when a list containing set of integers is passed, this function returns
    two lists within a list one containing odd integers and the other containing even integers'''
    if not isinstance(list_int, list):
        raise TypeError("unsupported format, pass a list")
    for i in list_int:
        if not is_int(i):
            raise TypeError("unsupported format, pass integers inside the list")
    odd_list = []
    even_list = []
    for i in list_int:
        if i % 2 != 0:
            odd_list.append(i)
        else:
            even_list.append(i)
    return [odd_list, even_list]