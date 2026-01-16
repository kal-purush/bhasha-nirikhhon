package com.coding.challenge.sorting;

import java.util.Arrays;

class MinDiffBtwLargestAndSmallestValInThreeMoves {
	MinDiffBtwLargestAndSmallestValInThreeMoves() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 5, 3, 2, 4 };
		assert minDifference(nums1) == 0;

		final int[] nums2 = { 1, 5, 0, 10, 14 };
		assert minDifference(nums2) == 1;

		final int[] nums3 = { 3, 100, 20 };
		assert minDifference(nums3) == 0;
	}

	static int minDifference(int[] nums) {
		if (nums.length <= 4)
			return 0;
		Arrays.sort(nums);
		final int n = nums.length;
		// simulating all the possible cases, 4 in this case and find the min.
		int diff = Math.min(nums[n - 4] - nums[0], nums[n - 3] - nums[1]);
		diff = Math.min(diff, nums[n - 2] - nums[2]);
		diff = Math.min(diff, nums[n - 1] - nums[3]);
		return diff;
	}
}