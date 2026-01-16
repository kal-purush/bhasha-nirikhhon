# Small proejct to get myself back on track

# I want it to be able to calculate a state's status after each tick of resistance.
# Like, have it make a table for it, given its stats.

import math


# Variables
working = True


# get Factories
print ("What's the factory count, the defense percentage and the resistance?")
while working:
    factories = input()
    
    try:
        factories = float(factories)
        working = False
    except:
        print ("Factory count needs to be an int.")


# get Defense
working = True
print ("Good, good. The defense?")
while working:
    defense = input()
    
    try:
        defense = float(defense)
        working = False
    except:
        print ("Defense percentage needs to be an int.")


# get Resistance
working = True
print ("Lastly, the resistance percentage.")
while working:
    resistance = input()
    
    try:
        resistance = float(resistance)
        working = False
    except:
        print ("Resistance percentage needs to be an int.")


print ("Result:")
print ("\tFactories: [", round(factories * resistance * 0.01), "/", round(factories), "]", sep = '')
print ("\tDefense: [", round (defense * resistance * 0.01), "/", round(defense), "]", sep = '')


# writing to file
file = open("HoiTPG.txt", "w+")
file.write("Hi!")

print ("Howdy!")








# HANDY STUFF


# example stuff
# Southern Kora - Plains [4(9)F | 11 (20)% D], Resistance 45%

# how to input 
# input1 = input() 