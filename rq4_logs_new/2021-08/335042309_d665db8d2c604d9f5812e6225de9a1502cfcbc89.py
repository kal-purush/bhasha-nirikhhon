def largest_sum(nums):
    n = len(nums)

    if n==1:
        return nums[0]

    opt = [0]*n 
    opt[0] = nums[0]
    maxsum = opt[0]

    for i in range(1,n):
        opt[i] = max(nums[i], opt[i-1]+nums[i])
        maxsum = max(maxsum, opt[i])

    return maxsum

nums = [-2,1,-3,4,-1,2,1,-5,4]
print(largest_sum(nums))

nums = [1]
print(largest_sum(nums))

nums = [5,4,-1,7,8]
print(largest_sum(nums))

nums = [5,-1,-1,-1,-1,12,-10]
print(largest_sum(nums))

nums = [-1,-1,-1,-1,-1,-1]
print(largest_sum(nums))

nums = [-1,-2]
print(largest_sum(nums))