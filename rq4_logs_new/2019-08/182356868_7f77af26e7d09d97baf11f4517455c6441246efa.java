package leet;


public class MaximumProductSubarray {
    public int maxProduct(int[] nums) {
            int highest = nums[0];
            int lowest = nums[0];


            for (int i = 1; i< nums.length; i++) {
                int number = nums[i];

                highest = Math.max(Math.max(highest, highest*number), number);
                lowest = Math.min(Math.min(lowest, lowest*number), number);

            }

            return highest;
        }

}