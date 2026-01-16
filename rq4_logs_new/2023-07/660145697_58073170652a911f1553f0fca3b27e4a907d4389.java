package rish.leets.grind.easy;

/*
 * 
 * Problem #: 704
 * Problem link: https://leetcode.com/problems/binary-search/
 * 
 * @author Rishabh Soni
 * 
 * Date Attempted: 01/07/2023
 *
 */
public class BinarySearch {

	public int search(int[] nums, int target) {

		int left = 0;
		int right = nums.length - 1;

		while (left <= right) {
			int mid = (int) Math.ceil((right + left) / 2);
			if (nums[mid] > target) {
				right = mid - 1;
			} else if (nums[mid] < target) {
				left = mid + 1;
			} else {
				return mid;
			}
		}

		return -1;
	}

}