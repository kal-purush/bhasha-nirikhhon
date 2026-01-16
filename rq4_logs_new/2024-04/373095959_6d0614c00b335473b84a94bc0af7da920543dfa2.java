package org.frank.leetcode.array.first.missing.positive.demo02;

public class FirstMissingPositiveSolution2 {
    public int firstMissingPositive(int[] nums) {
        int n = nums.length;

        // Step 1: Check if 1 is present in the array
        boolean containsOne = false;
        for (int num : nums) {
            if (num == 1) {
                containsOne = true;
                break;
            }
        }
        if (!containsOne) return 1;

        // Step 2: Replace negative numbers, zeros, and numbers larger than n by 1s
        for (int i = 0; i < n; i++) {
            if (nums[i] <= 0 || nums[i] > n) {
                nums[i] = 1;
            }
        }

        // Step 3: Transform array using index as hash key
        for (int i = 0; i < n; ++i) {
            int a = Math.abs(nums[i]);
            if (a == n)
                nums[0] = -Math.abs(nums[0]);
            else
                nums[a] = -Math.abs(nums[a]);
        }

        // Step 4: Determine the first missing positive integer
        for (int i = 1; i < n; ++i) {
            if (nums[i] > 0)
                return i;
        }

        if (nums[0] > 0)
            return n;

        return n + 1;    
    }
}