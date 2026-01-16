class Solution {
    public long countFairPairs(int[] nums, int lower, int upper) {

        Arrays.sort(nums);
        long upperLimit = findPairs(nums, upper);
        long lowerLimit = findPairs(nums, lower-1);

        return upperLimit - lowerLimit;
        
    }

    public long findPairs(int[] nums, int target){
        long pairs = 0;
        int left = 0;
        int right = nums.length - 1;

        while(left < right){
            if(nums[left] + nums[right] <= target){
                pairs += (right - left);
                left++;
            } else {
                right--;
            }
        }
        return pairs;
    }
}