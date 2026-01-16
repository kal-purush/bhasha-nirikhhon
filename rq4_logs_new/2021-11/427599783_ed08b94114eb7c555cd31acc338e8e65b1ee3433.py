# Given a list of integer nums, return the earliest index i 
# such that the sum of the numbers left of i 
# is equal to the sum of numbers right of i. 
# If theres no solution, return -1 
# Sum of an empty list is defined to be 0.

#time killer
# def solve(nums):
# 	if len(nums) <=1:
# 		return 0 
# 	if len(nums) == 2:
#         if nums[0] == 0 and  nums[1] == 0 
#             return 0
#         if nums[0] != 0 and  nums[1] == 0 
#             return 0
#         if nums[0] == 0 and  nums[1] != 0 
#         return 1
# 	for index in range(0, len(nums)):
# 		if sum(nums[:index]) == sum(nums[index+1:]):
# 			return index
# 	return -1 


# res = smallest_index([2, 3, 4, 0, 5, 2, 2])
# print(res)



"""
Problem description:
https://binarysearch.com/problems/Index-with-Equal-Left-and-Right-Sums
"""

def solve(nums):
    right_sum = sum(nums[1:])
    left_sum = 0

    if right_sum == 0:
        return 0 # special case with sum of all elements zero except first

    i = 0
    j = 1
    while j < len(nums):
        left_sum += nums[i]
        right_sum -= nums[j]

        if left_sum == right_sum:
            print(i+1, j)
            return i+1
        j += 1
        i += 1
    return -1

def res_check(expect, actual, input):
    if expect != actual:
        print('error : expecting %d , got %d for input %s' % (expect, actual, input))
    print(expect, actual)

ip = [2, 3, 4, 0, 5, 2, 2]
res = solve(ip)
res_check(3, res, ip)

ip = [1, -2, 2]
res = solve(ip)
res_check(0, res, ip)

ip = [0]
res = solve(ip)
res_check(0, res, ip)

ip = [-1] * 60001
res = solve(ip)
res_check(30000, res, ip)