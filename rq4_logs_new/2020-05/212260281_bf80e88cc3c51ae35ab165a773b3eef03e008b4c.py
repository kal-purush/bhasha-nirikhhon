# DESCRIPTION
# Given an array of integers and an integer k, you need to find
# the total number of continuous subarrays whose sum equals to k.

# EXAMPLE:
# Input: nums = [1,1,1], k = 2
# Output: 2

class Solution:
    def subarraySum(self, nums: List[int], k: int) -> int:
        '''
        Time: O(N), where N is the number of nums
        Space: O(N), space for the hashmap to hold nums
        '''
        if not nums:
            return 0

        count = curr_sum = 0

        # map to hold seen values
        my_map = {}

        for i in nums:
            # continuous sum
            curr_sum += i

            if curr_sum == k:
                count += 1

            # value = curr_sum - k
            # if a value exists in the table
            # then there is a sum that has been seen before
            if curr_sum - k in my_map:
                # using the count in map rather than just inc by 1
                # because there can be multiple subarrays seen
                count += my_map[curr_sum - k]

            if curr_sum in my_map:
                my_map[curr_sum] += 1
            else:
                my_map[curr_sum] = 1

        return count