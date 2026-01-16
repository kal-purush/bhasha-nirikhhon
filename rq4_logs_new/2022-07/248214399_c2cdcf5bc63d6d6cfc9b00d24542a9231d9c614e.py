# https://www.acmicpc.net/problem/10819

from sys import stdin
from itertools import permutations
input = stdin.readline

N = int(input())
nums = list(map(int, input().split()))
ans = -1000
p_nums = list(permutations(nums, N))

for p in p_nums:
    ans = max(ans, sum([abs(p[i]-p[i+1]) for i in range(N-1)]))

print(ans)