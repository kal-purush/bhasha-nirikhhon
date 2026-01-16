class Solution:
    def sortColors(self, nums: List[int]) -> None:
        """
        Do not return anything, modify nums in-place instead.
        """
        cnt = [0] * 3
        for i in range(len(nums)):
            cnt[nums[i]] += 1
            
        i = 0
        for num, c in enumerate(cnt):
            for _ in range(c):
                nums[i] = num
                i += 1