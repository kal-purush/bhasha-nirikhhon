package org.example.leetcode.removeduplicatesfromsortedarray;

/**
 * https://leetcode.com/problems/remove-duplicates-from-sorted-array/description/
 */
public class Executor {

    public static void main(String[] args) {
        int[] nums = {0, 0, 1, 1, 1, 2, 2, 3, 3, 4};
        System.out.println(removeDuplicates(nums));
    }

    public static int removeDuplicates(int[] nums) {
        if (nums.length == 0) {
            return 0;
        }

        int j = 0;
        for (int i = 1; i < nums.length; i++) {
            if (nums[j] != nums[i]) {
                j = j + 1;
                nums[j] = nums[i];
            }
        }

        return j + 1;
    }

    public static int removeDuplicatesMySolution(int[] nums) {
        if (nums.length == 0) {
            return 0;
        }

        int k = 1;

        for (int i = 0; i < nums.length; i++) {
            int num = nums[i];
            boolean isKModified = false;

            for (int j = i + 1; j < nums.length; j++) {
                if (nums[j] > num) {
                    nums[i + 1] = nums[j];
                    k++;
                    isKModified = true;
                    break;
                }
            }
            if (!isKModified) {
                return k;
            }
        }

        return k;
    }

}