package DP;

/**
 * Leetcode 53 - Maximum SubArray
 * Find the contiguous subarray within an array (containing at least one number) which has the largest sum.
 * For example, given the array [-2, 1, -3, 4, -1, 2, 1, -5, 4],
 * the contiguous subarray [4, -1, 2, 1] has the largest sum = 6
 *
 * Created by hashrambo on 3/29/2017.
 */
public class MaximumSubArray {

    /**
     * max subarray is a dp problem. The solution is to break it down to subproblem first.
     * So this array has i elements. The point is to keep track of the maximum so far for 1... i-1 element.
     * The algorithm works like this. First we have two variables for tracking purposes. One is to keep track
     * of the maximum for the previously added sum to the current element. The other one is to keep track of
     * the maximum for the maximum so far with the max value from previous iteration with the previous tracking
     * variable, that is the max of the sum with the added current element in this iteration.
     *
     * Time Complexity: O(n)
     * Space Complexity: O(n)
     * @param nums
     * @return
     */
    private static int maxSubArray (int[] nums) {
        final int size = nums.length;

        if (size == 0)
            return 0;

        int sum = nums[0];
        int max = nums[0];

        for (int i=0; i<size; i++) {
            sum = Math.max(nums[i], sum + nums[i]);
            max = Math.max(max, sum);
        }
        return max;
    }

    public static void main (String [] args) {
        int [] nums = new int [] {-2, 1, -3, 4, -1, 2, 1, -5, 4}; //6
        System.out.println(maxSubArray(nums));
    }
}