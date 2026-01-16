# DESCRIPTION
# Given an array nums of n integers where n > 1,
# return an array output such that output[i] is equal to the
# product of all the elements of nums except nums[i].

# EXAMPLE:
# Input:  [1,2,3,4]
# Output: [24,12,8,6]

# Constraint: It's guaranteed that the product of the elements
# of any prefix or suffix of the array (including the whole array)
# fits in a 32 bit integer.

# Note: Please solve it without division and in O(n).

class Solution:
    def productExceptSelf(self, nums: List[int]) -> List[int]:
        '''
        Time: O(N), iterates over the nums list twice
        Space: O(1), no auxiliry data structure only the output array
        '''

        N = len(nums)
        output = [1]

        # takes the product of everything that came before it
        for i in range(1, N):
            output.append(nums[i-1]*output[i-1])
        # Current Output array: [1,1,2,6]

        product_right = 1

        # starting from the end of the array go backward
        # takes the product that has come before it
        for j in range(N-1, -1, -1):
            output[j] = (output[j]*product_right)
            product_right *= nums[j]

        return output