package com.coding.challenge.math;

class MissingNumber {
	MissingNumber() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 3, 0, 1 };
		assert missingNumber(nums1) == 2;

		final int[] nums2 = { 0, 1 };
		assert missingNumber(nums2) == 2;

		final int[] nums3 = { 1, 8, 4, 6, 2, 0, 9, 7, 5 };
		assert missingNumber(nums3) == 3;
	}

	static int missingNumber(int[] nums) {
		final int n = nums.length;
		int all = 0;
		for (int i = 0; i <= n; i++)
			all = all + i;

		int missing = 0;
		for (int i = 0; i < n; i++)
			missing = missing + nums[i];

		return all - missing;
	}
}