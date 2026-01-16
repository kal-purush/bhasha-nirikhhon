package com.github.lucaseo90.ps.day12;

public class No0713 {
    public int numSubarrayProductLessThanK(int[] nums, int k) {
        int lessThanK = 0;
        for (int i = 0; i < nums.length; i++) {
            int product = nums[i];
            if (product < k) {
                lessThanK++;
            }
            for (int j = i + 1; j < nums.length; j++) {
                product *= nums[j];
                if (product < k) {
                    lessThanK++;
                } else {
                    break;
                }
            }
        }
        return lessThanK;
    }
}