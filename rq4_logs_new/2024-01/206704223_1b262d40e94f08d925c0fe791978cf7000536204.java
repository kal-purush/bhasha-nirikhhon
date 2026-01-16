package com.coding.challenge.dp;

import java.util.Arrays;

class MaximumSumOfThreeNonOverlappingSubarrays {
	MaximumSumOfThreeNonOverlappingSubarrays() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 1, 2, 1, 2, 6, 7, 5, 1 };
		final int[] a1 = { 0, 3, 5 };
		assert Arrays.equals(a1, maxSumOfThreeSubarrays(nums1, 2));

		final int[] nums2 = { 1, 2, 1, 2, 1, 2, 1, 2, 1 };
		final int[] a2 = { 0, 2, 4 };
		assert Arrays.equals(a2, maxSumOfThreeSubarrays(nums2, 2));

		final int[] a3 = { 1, 3, 5 };
		assert Arrays.equals(a3, maxSumOfThreeSubarrays(nums2, 1));
	}

	static int[] maxSumOfThreeSubarrays(int[] nums, int k) {
		final int n = nums.length;
		final int[] preSum = new int[n + 1];
		for (int p = 1; p <= n; p++)
			preSum[p] = preSum[p - 1] + nums[p - 1];

		final int[][] s = new int[4][n + 1];
		for (int i = 1; i <= 3; i++)
			for (int j = i * k; j <= n; j++)
				if (j >= i * k)
					s[i][j] = Math.max(s[i - 1][j - k] + preSum[j] - preSum[j - k], s[i][j - 1]);

		// constructing an optimal solution.
		return constructStartingPositions(s, k);
	}

	private static int[] constructStartingPositions(int[][] s, int k) {
		// start from top-right position.
		final int[] a = new int[s.length - 1];
		for (int i = s.length - 1, j = s[0].length - 1, p = i - 1; i > 0; i--) {
			while (s[i][j - 1] == s[i][j])
				j = j - 1;
			a[p] = j - k;
			p = p - 1;
			j = j - k;
		}
		return a;
	}
}