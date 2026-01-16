package interview.leetcode.prob;

/**
 * You are given an integer array nums and a positive integer k.

Return the number of subarrays where the maximum element of nums appears at least k times in that subarray.

A subarray is a contiguous sequence of elements within an array.

 

Example 1:

Input: nums = [1,3,2,3,3], k = 2
Output: 6
Explanation: The subarrays that contain the element 3 at least 2 times are: [1,3,2,3], [1,3,2,3,3], [3,2,3], [3,2,3,3], [2,3,3] and [3,3].
Example 2:

Input: nums = [1,4,2,1], k = 3
Output: 0
Explanation: No subarray contains the element 4 at least 3 times.
 

Constraints:

1 <= nums.length <= 105
1 <= nums[i] <= 106
1 <= k <= 105
Accepted
27,661
Submissions
54,455
 * @author jojo
 * Mar. 28, 2024 8:55:07 p.m.
 */
public class CountSubArrayWhereMaxAppearsAteastKTimes {
	public long countSubarrays(int[] nums, int k) {
        int max = nums[0];
        
        for(int n : nums){
            max = Math.max(max, n);
        }
        
        long result = 0L, maxElementCount = 0L;
        
        for(int i=0, j=0; i<nums.length; i++){
            if(nums[i] == max){
                maxElementCount++;
            }
            
            while(maxElementCount == k){
                if(nums[j] == max){
                    maxElementCount--;
                }
                
                j++;                
            }
            
            result += j;
        }
        
        return result;
    }
}