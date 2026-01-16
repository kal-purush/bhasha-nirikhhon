'''
Suppose an array sorted in ascending order is rotated at some pivot unknown to you beforehand.

(i.e., [0,1,2,4,5,6,7] might become [4,5,6,7,0,1,2]).

You are given a target value to search. If found in the array return its index, otherwise return -1.

You may assume no duplicate exists in the array.

Your algorithm's runtime complexity must be in the order of O(log n).

Example 1:

Input: nums = [4,5,6,7,0,1,2], target = 0
Output: 4
Example 2:

Input: nums = [4,5,6,7,0,1,2], target = 3
Output: -1
'''


def search(A: List[int], target:int) -> int:
    n = len(A)
    l = 0
    r = n - 1

    while l < r:
        mid = l + (r - l) // 2
        if A[mid] == target:
            return mid
        if A[l] < A[mid]:
            if A[l] <= target and target < A[mid]:
                r = mid - 1
            else:
                l = mid + 1
        else:
            if A[r] >= target and target > A[mid]:
                l = mid + 1
            else:
                r = mid - 1
    return -1