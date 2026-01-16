package org.frank.leetcode.array.find.pivot.index.demo02;

public class FindPivotIndexSolution2 {

    public int pivotIndex(int[] nums) {
        int length = nums.length;
        int pivotIndex = -1;
        int totalSum = 0;
        int leftSum = 0;
        for (int num : nums) {
            totalSum += num;
        }
        for (int i = 0; i < length; i++) {
            if (leftSum == totalSum - leftSum - nums[i]) {
                pivotIndex = i;
                break;
            }
            leftSum += nums[i];
        }
        return pivotIndex;
    }
}