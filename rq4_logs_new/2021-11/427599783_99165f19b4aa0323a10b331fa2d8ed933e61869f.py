"""
https://binarysearch.com/problems/Minimum-Cost-Sort
"""

def solve(nums):
    nums_new = nums[:]
    nums_new.sort()

    asc_cost = sum([abs(i-j) for i,j in zip(nums, nums_new)])

    nums_new.sort(reverse=True)
    dsc_cost = sum([abs(i-j) for i,j in zip(nums, nums_new)])
    return min(asc_cost, dsc_cost)

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

ip = [1, 4, 3]
res_check(2, solve(ip), ip)


ip = [7,3,5]
res_check(4, solve(ip), ip)