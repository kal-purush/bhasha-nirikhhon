# -*- coding: utf-8 -*-
from collections import Counter

n, m = map(int, raw_input().split())

c_n = Counter(map(lambda x: x % 5, xrange(1, n + 1)))
c_m = Counter(map(lambda x: x % 5, xrange(1, m + 1)))

print(sum([
    c_n[0] * c_m[0],
    c_n[1] * c_m[4],
    c_n[2] * c_m[3],
    c_n[3] * c_m[2],
    c_n[4] * c_m[1],
]))