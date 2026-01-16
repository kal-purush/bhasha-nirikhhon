package leetcode.arrayandhashing.easy;

import java.util.HashMap;
import java.util.Map;

public class TwoSum {

    //  Time complexity: O(n ^ 2)
    //  Space complexity:
    //          Extra:O(1)
    //          Algo:O(n)
    //  Pattern: Brute force.
    public static int[] twoSumBruteForce(int[] nums, int target) {
        for (int leftIndex = 0; leftIndex < nums.length; leftIndex++) {
            for (int rightIndex = leftIndex + 1; rightIndex < nums.length; rightIndex++) {
                if (nums[leftIndex] + nums[rightIndex] == target) {
                    return new int[]{leftIndex, rightIndex};
                }
            }
        }
        return null;
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(n)
    //  Pattern: Hashing / Two-pass or One-pass Hash Table.
    public static int[] twoSumOptimised(int[] nums, int target) {
        Map<Integer, Integer> valueToIndex = new HashMap<>();

        for (int currentIndex = 0; currentIndex < nums.length; currentIndex++) {
            int currentValue = nums[currentIndex];
            int neededValue = target - currentValue;

            if (valueToIndex.containsKey(neededValue)) {
                return new int[] {currentIndex, valueToIndex.get(neededValue)};
            }
            valueToIndex.put(currentValue, currentIndex);
        }
        return new int[] {};
    }
}