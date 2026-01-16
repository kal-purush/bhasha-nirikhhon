# -*- coding: utf-8 -*-
"""
Editor: Zhao Xinlu
School: BUPT
Date: 2018-03-27
算法思想：排序数组去重复元素II
"""
class Solution(object):
    def removeDuplicates(self, nums):
        """
        :type nums: List[int]
        :rtype: int
        """
        if len(nums) <= 2:
            return len(nums)

        i = 0
        count = 0
        for j in range(0, len(nums)):
            if j > 0 and nums[j] == nums[j-1]:
                count += 1
                if count >= 3:
                    continue
            else:
                count = 1

            nums[i] = nums[j]
            i += 1

        return i

if __name__ == '__main__':
    print Solution().removeDuplicates([1, 1, 1, 2, 2, 3])