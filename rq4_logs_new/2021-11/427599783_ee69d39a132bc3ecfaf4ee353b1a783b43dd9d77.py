"""
Problem description:
https://binarysearch.com/problems/Arithmetic-Sequence-Permutation
"""

def solve(nums):
    nums.sort()
    x1,x2 = nums[0], nums[1]
    diff = x2 - x1
    for i in nums[2:]:
        new_diff = i - x2
        if new_diff != diff:
            return False
        else:
            x2 = i
    return True

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %d , got %d for input %s' % (expect, actual, input))

ip = [5, 1, 5, 1, 5, 1]
res = solve(ip[:])
res_check(3, res, ip)

ip = [7, 1, 5, 3]
res = solve(ip[:])
res_check(0, res, ip)