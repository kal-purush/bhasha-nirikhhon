def find_start_end(nums, target):

    n = len(nums)

    if n==0:
        return [-1,-1]

    l = 0
    r = n-1
    f = 0
    while l<=r:
        start = (l+r)//2

        if nums[start]==target:
            f = 1
            if start<=0 or nums[start-1]!=target: # We've reached left most match
                break 
            elif nums[start-1]==target: # We look on the left side
                r = start-1
        elif nums[start]>target:
            r = start-1
        else:
            l = start+1 

    l = 0
    r = n-1
    # f2 = 0
    while l<=r:
        end = (l+r)//2

        if nums[end]==target:
            # f2 = 1
            if end>=n-1 or nums[end+1]!=target: # We've reached right most match
                break
            elif nums[end+1]==target:
                l = end+1
        elif nums[end]>target:
            r = end-1
        else:
            l = end+1
    
    if f==1: # We have found the target
        return [start, end]
    else:
        return [-1, -1]

nums = [5,7,7,8,8,10]
target = 8
find_start_end(nums, target)

nums = [5,7,7,8,8,10]
target = 6
find_start_end(nums, target)

nums = []
target = 0
find_start_end(nums, target)

nums = [-1,-1,-1,2,3,4,4,4,7,7,9,10,11,12,12,12,12]
find_start_end(nums, -1)
find_start_end(nums, 4)
find_start_end(nums, 12)