
# B. front_x
# Given a list of strings, return a list with the strings
# in sorted order, except group all the strings that begin with 'x' first.
# e.g. ['mix', 'xyz', 'apple', 'xanadu', 'aardvark'] yields
# ['xanadu', 'xyz', 'aardvark', 'apple', 'mix']
# Hint: this can be done by making 2 lists and sorting each of them
# before combining them.





# from ctypes.wintypes import WORD
# from re import I
# from socket import RDS_RDMA_INVALIDATE
# from tkinter import W


word=['mix', 'xyz', 'apple', 'xanadu', 'aardvark']
def front_x(word):
    list_x=[]
    list_oth=[]
    for i in word:
        if i.startswith('x'):
            list_x.append(i)
        else:
            list_oth.append(i)
    return sorted(list_x)+sorted(list_oth)            
print(front_x(word))
