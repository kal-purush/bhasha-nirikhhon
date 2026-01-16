class Solution:
    def maxAscendingSum(self, nums: List[int]) -> int:
        summ=nums[0]
        maxSum=0
        for i in range(1,len(nums)):
            if nums[i]>nums[i-1]:
                summ+=nums[i]
            else:
                maxSum=max(maxSum,summ)
                summ=nums[i]
        maxSum=max(summ,maxSum)
        return maxSum
        