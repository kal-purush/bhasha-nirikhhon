class Solution:
    def maxSubArray(self, nums: List[int]) -> int:
        '''
        Time: O(N), Only one iterations is needed of the array
        Space: O(1), no extra data structures used
        Kadane's Algorithm
        '''
        # current and best max starts at the first element in list
        curr_max = best_max = nums[0]


        for i in range(1,len(nums)):
            # has the max come before or does the current element change that
            curr_max = max(nums[i], nums[i]+curr_max)

            # update the max just make sure we didnt lose the best max
            if curr_max > best_max:
                best_max = curr_max

        return best_max