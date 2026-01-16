"""
https://binarysearch.com/problems/Sum-of-Two-Numbers
"""
def solve(nums, k):
    if len(nums) == 1:
        return False
    d = {}
    for i in nums:
        if i in d:
            d[i] += 1
        else:
            d[i] = 1

    for i in nums:
        #print(k-i, d)
        if (k-i in d and k-i != i) or (k-i in d and d[i] == 2):
            return True
    return False

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %d , got %d for input %s' % (expect, actual, input))

ip = [35, -8, 18, 3, 22]
k = -5
res = res_check(True, solve(ip, k), ip)

ip = [22]
k = 44 # same element not allowed, so False.
res = res_check(False, solve(ip, k), ip)

ip = [22, 1, 34, 4, 22]
k = 44
res = res_check(True, solve(ip, k), ip)