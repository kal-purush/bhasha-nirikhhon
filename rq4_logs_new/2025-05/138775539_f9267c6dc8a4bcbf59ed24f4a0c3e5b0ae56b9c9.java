
package my.com.prefixsum;
import java.util.HashMap;

// Contiguous Array
// Given a binary array nums, return the maximum length of a contiguous subarray with an equal number of 0 and 1.
// Approach: Use prefix sum to convert 0s to -1s and then find the longest subarray with sum 0 using a hashmap.

// Time Complexity: O(n)
// Space Complexity: O(n)       
class Solution {
    public int findMaxLength(int[] nums) {
        
        if ( nums.length ==1 )
            return 0;
        
        if ( nums.length ==2  ){
            if (nums[0]!= nums[1])
                return 2;
            else
                return 0;
        }
        
        HashMap<Integer , Integer> hm = new HashMap();

        int sum =0 , maxLen = 0 ;
        hm.put(0, -1);
        for ( int i =0 ; i < nums.length; i++){
            if ( nums[i] == 0){
                sum += -1;
            } else {
                sum += 1;
            }

            if ( hm.containsKey( sum)) {
                // calculate the length of SubSrray , compare with maxlen
                
                int len  =  i - hm.get(sum);
               
                maxLen = Math.max(len, maxLen); 

            } else {
                hm.put( sum, i);
            }
        }

        return maxLen;
    }
}