package org.frank.leetcode.array.find.the.duplicate.number.demo02;

public class FindTheDuplicateNumberSolution2 {
    public int findDuplicate(int[] nums) {
        // Phase 1: Finding the intersection point of the two runners.
        int tortoise = nums[0];
        int hare = nums[0];
        do {
            tortoise = nums[tortoise];
            hare = nums[nums[hare]];
        } while (tortoise != hare);

        // Phase 2: Finding the entrance to the cycle (duplicate number).
        tortoise = nums[0];
        while (tortoise != hare) {
            tortoise = nums[tortoise];
            hare = nums[hare];
        }

        return hare;
    }

    public static void main(String[] args) {
        FindTheDuplicateNumberSolution2 solution2 = new FindTheDuplicateNumberSolution2();
        int[] nums = {2,5,9,6,9,3,8,9,7,1};        
        solution2.findDuplicate(nums);
    }
}