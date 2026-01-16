public class Solution {
    public boolean canSortArray(int[] nums) {
        int lastGroupMax = Integer.MIN_VALUE;
        int max = nums[0];
        int lastBit = Integer.bitCount(nums[0]);
        for(int i = 1; i < nums.length; i++){
            int currentBit = Integer.bitCount(nums[i]);
            if(currentBit == lastBit){
                max = Math.max(max, nums[i]);
            } else {
                lastGroupMax = max;
                max = nums[i];
                lastBit = currentBit;
            }
            if(nums[i] < lastGroupMax){
                return false;
            }
        }
        return true;
    }
}