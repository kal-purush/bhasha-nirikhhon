'''Parameters, Unpacking, Variables

give your script inputs on the terminal, then go with argv
'''

from sys import argv # import argv from sys module

script, first, second, third = argv # short for arguments vector
                                    # Take whatever is in argv, unpack it, and assign it to all of these variables on the left in order.
                                    # it gets inputs from terminal command directly.
                                    # ex) python3 ex13.py first second, third
                                    #             script                        

print("The script is called:", script)
print("Your first variable is:", first)
print("Your second variable is:", second)
print("Your third variable is:", third)