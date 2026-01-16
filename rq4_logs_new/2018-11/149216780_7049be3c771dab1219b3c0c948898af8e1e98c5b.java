package com.hussir.leetcode.part004;

/**
 * Description:
 *
 *      Implement next permutation, which rearranges numbers into the lexicographically next greater permutation of numbers.
 *
 *      If such arrangement is not possible, it must rearrange it as the lowest possible order (ie, sorted in ascending order).
 */
public class Question0031 {

    public void nextPermutation(int[] nums) {

        if (nums.length == 0 || nums.length == 1) return;

        boolean isBagSeq = true;

        int idxI = -1;
        int idxJ = -1;
        for (int i = nums.length - 2; i >= 0; i--) {
            int diff = Integer.MAX_VALUE;
            for (int j = nums.length - 1; j > i; j--) {
                if (nums[i] < nums[j]) {

                    isBagSeq = false;

                    if (diff > (nums[j] - nums[i])) {
                        diff = nums[j] - nums[i];
                        idxI = i;
                        idxJ = j;
                    }
                }
            }

            if (!isBagSeq) {
                break;
            }
        }

        if (!isBagSeq) {
            swap(nums, idxI, idxJ);
            reverse(nums, idxI + 1, nums.length - 1);
        } else {

            int i = 0;
            int j = nums.length - 1;

            reverse(nums, i, j);
        }
    }

    private void swap(int[] nums, int i, int j) {
        int temp = nums[i];
        nums[i] = nums[j];
        nums[j] = temp;
    }

    private void reverse(int[] nums, int leftIdx, int rightIdx) {
        while (leftIdx < rightIdx) {
            int temp = nums[leftIdx];
            nums[leftIdx++] = nums[rightIdx];
            nums[rightIdx--] = temp;
        }
    }
}