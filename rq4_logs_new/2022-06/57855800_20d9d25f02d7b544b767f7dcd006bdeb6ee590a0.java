package com.github.lucaseo90.algo.days21.day1;

public class FindFirstAndLastPosition {
    public int[] searchRange(int[] nums, int target) {
        int left = 0;
        int right = nums.length - 1;

        int minIndex = -1;
        int maxIndex = -1;
        while (left <= right) {
            if (minIndex != -1 && maxIndex != -1) {
                break;
            }
            if (minIndex == -1 && nums[left] == target) {
                minIndex = left;
            }

            if (maxIndex == -1 && nums[right] == target) {
                maxIndex = right;
            }
            if (minIndex == -1) {
                left++;
            }
            if (maxIndex == -1) {
                right--;
            }
        }

        if (minIndex == -1 && maxIndex != -1) {
            minIndex = maxIndex;
        } else if (minIndex != -1 && maxIndex == -1) {
            maxIndex = minIndex;
        }

        return new int[]{minIndex, maxIndex};
    }
}