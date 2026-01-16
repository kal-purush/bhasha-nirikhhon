/*
34. Find First and Last Position of Element in Sorted Array
Medium
16.5K
393
Companies
Given an array of integers nums sorted in non-decreasing order, find the starting and ending position of a given target value.

If target is not found in the array, return [-1, -1].

You must write an algorithm with O(log n) runtime complexity.



Example 1:

Input: nums = [5,7,7,8,8,10], target = 8
Output: [3,4]
Example 2:

Input: nums = [5,7,7,8,8,10], target = 6
Output: [-1,-1]
Example 3:

Input: nums = [], target = 0
Output: [-1,-1]


Constraints:

0 <= nums.length <= 105
-109 <= nums[i] <= 109
nums is a non-decreasing array.
-109 <= target <= 109
 */
public class FindElementBothPostitionInArray {
        public int[] searchRange(int[] nums, int target) {
            int start = 0;
            int len = nums.length;
            int end = len - 1;
            int mid = 0;
            int result[] = {-1,-1};

            while(start <= end) {
                mid = (start + end) / 2;

                if(target == nums[mid]) {
                    if(nums[start] == target) {
                        result[0] = start;
                    }
                    else {
                        for(int i = mid - 1; i >= start; i--) {
                            if(nums[i] != target) {
                                result[0] = i + 1;
                                break;
                            }
                        }
                    }

                    if(nums[end] == target) {
                        result[1] = end;
                    }
                    else {
                        for(int j = mid + 1; j <= end; j++) {
                            if(nums[j] != target) {
                                result[1] = j - 1;
                                break;
                            }
                        }
                    }
                    break;
                }

                if(target < nums[mid]) {
                    end = mid - 1;
                }
                else
                {
                    start = mid + 1;
                }

            }
            return result;
    }
}