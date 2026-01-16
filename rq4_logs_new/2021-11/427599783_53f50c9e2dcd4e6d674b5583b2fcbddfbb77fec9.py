"""
https://binarysearch.com/problems/List-Min-Replacement
"""

def solve(nums):
    cur_min = nums[0]
    for i in range(1, len(nums)):
        #print(i, cur_min)
        if nums[i] < cur_min:
            temp = nums[i]
            nums[i] = cur_min
            cur_min = temp
        else:
            nums[i] = cur_min
        #print(nums)
    nums[0] = 0
    return nums

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %d , got %d for input %s' % (expect, actual, input))

ip = [10, 5, 7, 9]
out = [0, 10, 5, 5]
res_check(out, solve(ip), ip)