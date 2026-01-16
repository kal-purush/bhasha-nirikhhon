package com.github.lucaseo90.ps.day01;

public class MonotonicArray {
    public boolean isMonotonic(int[] nums) {
        boolean isMonotonic = true;

        boolean isIncreasing = false;
        boolean isDecreasing = false;
        for (int i = 0; i < nums.length - 1; i++) {
            if(nums[i] < nums[i+1]) {
                isIncreasing = true;
            } else if(nums[i] > nums[i+1]) {
                isDecreasing = true;
            }

            if(isIncreasing && isDecreasing) {
                isMonotonic = false;
                break;
            }
        }
        return isMonotonic;
    }
}