package com.coding.challenge.kadanes;

class MaximumScoreOfSplicedArray {
	MaximumScoreOfSplicedArray() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 60, 60, 60 };
		final int[] nums2 = { 10, 90, 10 };
		assert maximumsSplicedArray(nums2, nums1) == 210;

		final int[] nums3 = { 20, 40, 20, 70, 30 };
		final int[] nums4 = { 50, 20, 50, 40, 20 };
		assert maximumsSplicedArray(nums3, nums4) == 220;

		final int[] nums5 = { 7, 11, 13 };
		final int[] nums6 = { 1, 1, 1 };
		assert maximumsSplicedArray(nums6, nums5) == 31;

		final int[] nums7 = { 28, 34, 38, 14, 30, 31, 23, 7, 28, 3 };
		final int[] nums8 = { 42, 35, 7, 6, 24, 30, 14, 21, 20, 34 };
		assert maximumsSplicedArray(nums7, nums8) == 288;
	}

	static int maximumsSplicedArray(int[] nums1, int[] nums2) {
		final int n = nums1.length;
		int s1 = 0;
		int s2 = 0;
		for (int i = 0; i < n; i++) {
			s1 = s1 + nums1[i];
			s2 = s2 + nums2[i];
		}
		final int delta1 = maxDeltaSubarr(nums1, nums2);
		final int delta2 = maxDeltaSubarr(nums2, nums1);
		return Math.max(s1, Math.max(s2, Math.max(s1 + delta2, s2 + delta1)));
	}

	private static int maxDeltaSubarr(int[] nums1, int[] nums2) {
		final int n = nums1.length;
		int maxDelta = Integer.MIN_VALUE;
		// maximum delta sum subarray finishing at index i.
		for (int i = 0, s = 0; i < n; i++) {
			// max delta sub array ends with i.
			final int delta = nums1[i] - nums2[i];
			// either append to the prefix or start from here.
			s = Math.max(s + delta, delta);
			maxDelta = Math.max(maxDelta, s);
		}
		return maxDelta;
	}
}