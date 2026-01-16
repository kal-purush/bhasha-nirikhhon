def bisect_search_helper(L,e,low, high):
    if high == low:
        return e == L[e]
    mid = (low + high) // 2
    if L[mid] == e:
        return True
    elif e < L[mid]:
        if low == mid: #nothing to search
            return False
        else:
            return bisect_search_helper(L,e,low, mid - 1)

    else:
        return bisect_search_helper(L,e,mid+1, high)

def bisect_search(L,e):
    if len(L) == 0: #no content in the given list
        return False

    else:
        return bisect_search_helper(L,e,0, len(L)-1)


nums = [1,2,334,342]
val = 334
print(bisect_search(nums, val))



#First Wrong Attemp:

#not logn because copying also takes linear time 
def binarySearch(nums, val):
    """
    input: a sorted list of numbers
    use binary search algorithm to return the first index location of the number provided by the user
    """
    if len(nums) == 0: 
        return False

    if len(nums) == 1:
        return nums[0] == val

    else:
        mid = len(nums) // 2 
        
        if nums[mid] == val: #program would run ok even without this condition
            return True

        if val < nums[mid]:
            found = binarySearch(nums[:mid], val)
            return found
        
        else:
            found = binarySearch(nums[mid:], val)
            return found



print(binarySearch(nums, val))