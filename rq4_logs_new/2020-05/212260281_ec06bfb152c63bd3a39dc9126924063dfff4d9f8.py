# DESCRIPTION
# Given a binary array, find the maximum length of a contiguous subarray with equal number of 0 and 1.

# EXAMPLE:
# Input: [0,1,0]
# Output: 2
# Explanation: [0, 1] (or [1, 0]) is a longest contiguous subarray with equal number of 0 and 1.

class Solution:
    def findMaxLength(self, nums: List[int]) -> int:
        '''
        Time: O(N), where N is the nums in list
        Space: O(N), for the hashmap
        '''
        # hashtable to put the size of count at each index
        counts = {}

        # intialize hashmap when their are no entries
        counts[0] = -1

        max_length = 0
        count = 0

        for i in range(len(nums)):
            if nums[i] == 1:
                count += 1
            else:
                count += -1


            if count in counts:
                # find the max between the current max
                # and curr index - prev index
                max_length = max(max_length, i - counts[count])
            else:
                # when the counter is not in the hashmap
                # create an entry
                counts[count] = i

        return max_length