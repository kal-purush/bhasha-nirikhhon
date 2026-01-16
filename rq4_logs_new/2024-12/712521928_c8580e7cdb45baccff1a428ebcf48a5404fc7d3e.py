"""
42. Trapping Rain Water

Given `n` non-negative integers representing an elevation map where the
width of each bar is 1, compute how much water it can trap after raining.
"""

"""
Example 1:
Input: height = [0,1,0,2,1,0,1,3,2,1,2,1]
Output: 6

Explanation: The above elevation map (black section) is represented by 
array [0,1,0,2,1,0,1,3,2,1,2,1]. In this case, 6 units of rain water 
(blue section) are being trapped.

Example 2:
Input: height = [4,2,0,3,2,5]
Output: 9
"""

class Solution:
    def trap(self, height: list[int]) -> int:
        if not height:
            return 0
        
        left_pointer = 0
        right_pointer = len(height) - 1
        max_left, max_right, trapped_water = 0, 0, 0

        while left_pointer < right_pointer:
            if height[left_pointer] < height[right_pointer]:
                if height[left_pointer] >= max_left:
                    max_left = height[left_pointer]
                else:
                    trapped_water += max_left - height[left_pointer]
                left_pointer += 1
            else:
                if height[right_pointer] >= max_right:
                    max_right = height[right_pointer]
                else:
                    trapped_water += max_right - height[right_pointer]
                right_pointer -= 1
        
        return trapped_water
                