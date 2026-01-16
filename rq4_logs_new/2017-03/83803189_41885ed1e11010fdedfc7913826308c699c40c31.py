"""
My solution to count instances, probably not thread safe
"""

class A():
    i = 0
    def __init__(self):
        A.i += 1
 
a = A()
print(a.i)

#1

b = A()
print(a.i)

#2
