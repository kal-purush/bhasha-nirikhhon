
'''
These are some examples for using the getRandomQubit() function.
This function will take 2 booleans, the first one being if you want to have negative numbers in your Qubit
and the second one being if you want irreale numbers in your Qubit.

Setting one or both to True does not mean the Qubit will have a negative number or irreal number each time. 
There is still a change both of those will not happen, because this is a 50% change.
'''

from Qubit import *

# Not irreal and not negative Qubit
q0 = getRandomQubit(False, False)
print("Example 1:\n" + str(q0) + "\n\n")

# Not irreal, but possibility for negative Qubit
q1 = getRandomQubit(True, False)
print("Example 2:\n" + str(q1) + "\n\n")

# Possibility for irreal numbers in Qubit, but not negative
q2 = getRandomQubit(False, True)
print("Example 3:\n" + str(q2) + "\n\n")

# Both irreal and negative are True and have a possibility to appear in the Qubit
q3 = getRandomQubit(True, True)
print("Example 3:\n" + str(q3) + "\n\n")