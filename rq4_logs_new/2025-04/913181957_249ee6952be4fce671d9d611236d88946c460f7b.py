#
# @lc app=leetcode id=2962 lang=python3
#
# [2962] Count Subarrays Where Max Element Appears at Least K Times
#

# @lc code=start
class Solution:
    def countSubarrays(self, nums: List[int], k: int) -> int:
        ln = len(nums)
        tg = max(nums)
        cnt = 0
        ans = 0
        start = 0

        for i in range(ln):
            if nums[i] == tg:
                cnt += 1

            while cnt >= k:
                ans += ln - i
                
                if nums[start] == tg:
                    cnt -= 1
                
                start += 1
        
        return ans
        
# @lc code=end
