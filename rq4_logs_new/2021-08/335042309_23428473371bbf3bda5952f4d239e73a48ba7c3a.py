def next_perm(nums):

    n = len(nums)

    if n==1:
        return nums 
    
    f = 0

    for i in range(n-1,0,-1):

        if nums[i-1]<nums[i]: # We get an inversion
            # Need to find smallest element greater than nums[i-1] in i to n-1
            min_ind = i 
            min_val = nums[i]
            f = 1 # Indicating an inversion exists
            for j in range(i,n):
                if nums[j]>nums[i-1] and nums[j]<min_val:
                    min_val = nums[j]
                    min_ind = j 

            nums[i-1], nums[min_ind] = nums[min_ind], nums[i-1]
            # nums = nums[:i] + sorted(nums[i:])
            nums[i:] = sorted(nums[i:])
            break 
    
    if f==0:
        nums.sort()
        nums = nums[::-1]

test1 = [1,2,3,4,9,8,7,2]
next_perm(test1)
print(test1)

test2 = [3,2,1]
next_perm(test2)
print(test2)