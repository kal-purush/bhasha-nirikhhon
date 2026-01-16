# Given a list of integers nums, return whether 
# you can rearrange the order of nums such that 
# the difference between every consecutive
# two numbers is the same.

def check_diff(given_nums):
	if len(nums) <= 2:
		return True
	given_nums = sorted(given_nums)
	prev_diff = given_nums[1] - given_nums[0]
	for index in range(1, len(given_nums)):
		diff = given_nums[index] - given_nums[index-1]
		if prev_diff != diff:
			return False
	return True 

res = check_diff([7, 1, 5, 3])
print(res)

res = check_diff([1, 5, 1, 5, 1, 5])
print(res)

