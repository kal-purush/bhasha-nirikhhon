import math

k, n, s, p = [int(x) for x in input().split()]

print( int(math.ceil(math.ceil(n/s) * k/p) ))