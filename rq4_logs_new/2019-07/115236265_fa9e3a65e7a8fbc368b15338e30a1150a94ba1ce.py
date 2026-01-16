#!/usr/bin/env python

def gcd(x, y):
    if y == 0:
        return x
    if x < y:
        x, y = y, x
    while y != 0:
        x, y = y, x % y
    return x

T = int(input())

for i in range(T):
    print("Case #{}: ".format(i + 1), end='')
    N, L = map(int, input().split())
    a = list(map(int, input().split()))
    b = [None] * (len(a) + 1)
    g = gcd(a[0], a[1])
    b[0] = a[0]/g
    t = b[0]
    for i, x in enumerate(a):
        b[i+1] = x / t
        t = b[i+1]
    n_c = {}
    for i, x in enumerate(sorted(list(set(b)))):
        n_c[x] = chr(i + ord('A'))
    for x in b:
        print(n_c[x], sep='', end='')
    print()