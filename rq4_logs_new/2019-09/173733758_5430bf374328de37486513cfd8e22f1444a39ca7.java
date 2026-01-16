package io.github.dlinov.leetcode;

import java.util.*;

public class Task300LongestIncreasingSubsequence {
    class Solution {

        public int lengthOfLIS(int[] nums) {
            if (nums.length == 0) {
                return 0;
            }
            final int[][] cache2 = new int[nums.length][nums.length + 1];
            for (int i = 0; i < nums.length; ++i) {
                Arrays.fill(cache2[i], -1);
            }
            return inner(nums, 0, -1, Integer.toString(nums[0]), cache2);
        }

        private int inner(int[] nums, int index, int prevIndex, String accS, int[][] cache2) {
            if (index >= nums.length) {
                return 0;
            }
            if (cache2[index][prevIndex + 1] == -1) {
                final int notTaken = inner(nums, index + 1, prevIndex, accS, cache2);
                int taken = 0;
                if (prevIndex < 0 || nums[index] > nums[prevIndex]) {
                    taken = 1 + inner(nums, index + 1, index, accS + "_" + nums[index], cache2);
                }
                cache2[index][prevIndex + 1] = Math.max(notTaken, taken);
            }
            return cache2[index][prevIndex + 1];
        }
    }
}