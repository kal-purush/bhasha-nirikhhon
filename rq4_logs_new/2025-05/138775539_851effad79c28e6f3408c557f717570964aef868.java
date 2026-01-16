pakage my.com.app;

import java.util.*;
// Minimum Operations to Make the Array Strictly Increasing
// Given an array of integers nums, you can perform the following operation any number of times:
// Choose two indices i and j where 0 <= i < j < nums.length and nums[i] >= nums[j].
// Increment nums[j] by 1.
// Return the minimum number of operations to make the array strictly increasing.


class Solution {
    public int minOperations(int[] nums) {
        
        int minOp = 0 ; 
        for ( int i=1 ; i < nums.length; i++) {
            
            if ( nums[i] <= nums[i-1]){
                
                minOp += nums[i-1] + 1 - nums[i];
                
                nums[i] = nums[i-1] +1;
            }
            
        }
        return minOp;
    }
}