package com.coding.challenge.greedy;

import java.util.Arrays;

class FormArrayByConcatenatingSubarraysOfAnotherArray {
	FormArrayByConcatenatingSubarraysOfAnotherArray() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] groups1 = { { 1, -1, -1 }, { 3, -2, 0 } };
		final int[] nums1 = { 1, -1, 0, 1, -1, -1, 3, -2, 0 };
		assert canChoose(groups1, nums1);

		final int[][] groups2 = { { 10, -2 }, { 1, 2, 3, 4 } };
		final int[] nums2 = { 1, 2, 3, 4, 10, -2 };
		assert !canChoose(groups2, nums2);

		final int[][] groups3 = { { 1, 2, 3 }, { 3, 4 } };
		final int[] nums3 = { 7, 7, 1, 2, 3, 4, 7, 7 };
		assert !canChoose(groups3, nums3);

		final int[][] groups4 = { { 21, 22, 21, 22, 21, 30 } };
		final int[] nums4 = { 21, 22, 21, 22, 21, 22, 21, 30 };
		assert canChoose(groups4, nums4);

		final int[][] groups5 = { { -5, 0 } };
		final int[] nums5 = { 2, 0, -2, 5, -1, 2, 4, 3, 4, -5, -5 };
		assert !canChoose(groups5, nums5);
	}

	static boolean canChoose(int[][] groups, int[] nums) {
		final int m = groups.length;
		final int n = nums.length;
		for (int i = 0, j = -1; i < m; i++) {
			final int l = groups[i].length;
			boolean found = false;
			while (j + 1 + l <= n && !found) {
				j = j + 1;
				found = Arrays.equals(groups[i], 0, l, nums, j, j + l);
			}
			if (found)
				j = j + l - 1;
			else
				return false;
		}
		return true;
	}
}