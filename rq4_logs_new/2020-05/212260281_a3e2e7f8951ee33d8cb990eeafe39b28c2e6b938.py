# DESCRIPTION
# Suppose an array sorted in ascending order is rotated
# at some pivot unknown to you beforehand.
#
# (i.e., [0,1,2,4,5,6,7] might become [4,5,6,7,0,1,2]).
#
# You are given a target value to search. If found in the array
# return its index, otherwise return -1.
# You may assume no duplicate exists in the array.

# EXAMPLE:
# Input: nums = [4,5,6,7,0,1,2], target = 0
# Output: 4

class Solution:
    def search(self, nums: List[int], target: int) -> int:
        '''
        Time: O(log N), modified binary search
        Space: O(log N), worst case for the recursion
        '''
        if not nums:
            return -1

        high = len(nums) - 1
        low = 0
        return self.mod_bin_search(nums, target, low, high)

    def mod_bin_search(self, nums, target, low, high):
        if low > high:
            return -1

        mid = low + (high - low) // 2

        if nums[mid] == target:
            return mid

        # left sorted
        if nums[low] <= nums[mid]:
            # if the target is in the sorted side
            if nums[mid] > target and nums[low] <= target:
                return self.mod_bin_search(nums, target, low, mid-1)
            else:
                return self.mod_bin_search(nums, target, mid+1, high)
        # right sorted
        else:
            if nums[mid] < target and nums[high] >= target:
                return self.mod_bin_search(nums, target, mid+1, high)
            else:
                return self.mod_bin_search(nums, target, low, mid-1)