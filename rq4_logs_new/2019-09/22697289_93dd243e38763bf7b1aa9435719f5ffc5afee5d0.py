from math import gcd
from functools import reduce

input()
a = list(map(int,input().split()))
ma = max(a) # time limit exceeded on test 5
z = reduce(gcd,[ma-x for x in a])
print(sum((ma-x)//z for x in a), z)