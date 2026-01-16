"""
https://binarysearch.com/problems/Wolves-of-Wall-Street
"""

def solve(nums):
    max_profit = 0
    cur = nums[0]
    for i in range(1, len(nums)):
        cur_profit = nums[i] - cur
        if cur_profit > 0:
            max_profit += cur_profit
        cur = nums[i]
    return max_profit

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %s , got %s for input %s' % (expect, actual, input))

res_check(7, solve([1, 5, 3, 4, 6]), [1, 5, 3, 4, 6])