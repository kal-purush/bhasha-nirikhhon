# -*- coding: utf-8 -*-

# Value assignment and basic calculation
a = 2
b = 3
c = a + b
print c
print 'a + b =', a + b
print 'a / b =', a/b
print 'a / b =', float(a)/b

import math
print 'a^b = ', math.pow(a, b)

# List
a = [1, 2, 3]
b = [4, 5]
print 'a:', a
print 'b:', b
print 'a + b =', a + b
print 'the 2nd element in a is: ', a[1]
# the index start from 0
print 'delete the first element of a: ', a.pop(0)
a
# a changed to [2, 3]
print 'now a is: ', a

# add a new element in the list
a.append(6)
print 'append a 6 to a: ', a

# extend b to a, similar to a + b
a.extend(b)
print 'extend a with b: ', a

# change element in the list
a = [1, 2]
# change the 2nd element to 3
a[1] = 3
print a

# Tuple
## Tuple is very similar to list, but elements in tuple cannot be mutated
a = (1, 2)
## use () to create tuple and [] to create list
print a[1]
# cannot be reassign - get error message
a[1] = 3

# Dictionary
# create dictionary: {key:value}
a = {'a': 1, 'b': 2}
print a['a']
print a.keys()
print a.values()
# print all items: paired  key with values
print a.items()

# For loop and while loop
this = [1, 2, 3]
for i in this:
    print i
    
for i in [1, 2, 3]:
    print i
    
a = 10
while a > 5:
    print a
    a = a -1
    

a = 10
while a > 5:
    print a
    a -= 1
    
a = 1
while a < 5:
    print a
    a += 1

a = 1
while True:
    a = a + 2
    if a > 10:
        print a
        break

# Function
## define a fuction use def function_name(argument)
## useless = T is the default setting
def maybe_useless_function(a, b, useless=True):
    if useless:
        return a, b
    else:
        return a + 1, b + 1

print maybe_useless_function(1, 2, useless=True)
print maybe_useless_function(1, 2, True)
print maybe_useless_function(1, 2)
print maybe_useless_function(1, 2, useless=False)
print maybe_useless_function(1, 2, False)














