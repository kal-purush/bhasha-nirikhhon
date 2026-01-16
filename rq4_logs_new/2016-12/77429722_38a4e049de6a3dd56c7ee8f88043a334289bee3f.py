#!/usr/bin/env python3

class Solution(object):
    def maxProduct(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        if not nums or len(nums) == 0:
            return 0

        local_max = nums[0]
        local_min = nums[0]
        global_max = nums[0]

        for num in nums[1:]:
            copy_max = local_max
            local_max = max(num, copy_max * num, local_min * num)
            local_min = min(num, copy_max * num, local_min * num)

            if global_max < local_max:
                global_max = local_max

        return global_max


s = Solution()
print(s.maxProduct([2, 2, 0.4, 3]))