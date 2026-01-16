import random
import time
import sys

accepted = [1 ,2 ,3]

loc_a = input('Where is location 1? ')
loc_b = input('Where is location 2? ')
loc_c = input('Where is location 3? ')

def distance_a (loc_a, loc_b, loc_c):
    diff1 = abs(loc_a - loc_b)
    diff2 = abs(loc_a - loc_c)

    return min(diff1, diff2)

def distance_b (loc_a, loc_b, loc_c):
    diff3 = abs(loc_b - loc_a)
    diff4 = abs(loc_b - loc_c)

    return min(diff3, diff4)

def distance_c (loc_a, loc_b, loc_c):
    diff5 = abs(loc_c - loc_a)
    diff6 = abs(loc_c - loc_b)

    return min(diff5, diff6)

print("1- City at", loc_a,'\n', "2- City at", loc_b, '\n', "3- City at", loc_c)
request = input('What city are you coming from')

while request not in accepted:
    print ('\nKindly answer with 1, 2, or 3')
    request = input('What city are you coming from')

if request == 1:
    distance_a (loc_a, loc_b, loc_c)

elif request == 2:
    distance_b (loc_a, loc_b, loc_c)

elif request == 3:
    distance_c (loc_a, loc_b, loc_c)

sys.exit()