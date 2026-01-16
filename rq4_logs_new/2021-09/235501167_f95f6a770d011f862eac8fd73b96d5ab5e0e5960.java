package easy;

/**
 * https://leetcode.com/problems/binary-search/
 *
 * Given an array of integers nums which is sorted in ascending order, and an integer target, write a function to search target in nums. If target exists, then return its index. Otherwise, return -1.
 *
 * You must write an algorithm with O(log n) runtime complexity.
 *
 *
 */
public class Binary_Search {
    public int search(int[] nums, int target) {
        return binSearch(nums, target, 0, nums.length - 1);
    }

    int binSearch(int[] nums, int target, int low, int high) {
        if (low > high) return -1;

        int mid = (low + high) / 2;

        if(nums[low] == target) return low;
        if(nums[high] == target) return high;
        if(nums[mid] == target) return mid;

        if(target < nums[mid]) {
            return binSearch(nums, target, low + 1, mid-1);
        } else {
            return binSearch(nums, target, mid + 1, high-1);
        }
    }
}