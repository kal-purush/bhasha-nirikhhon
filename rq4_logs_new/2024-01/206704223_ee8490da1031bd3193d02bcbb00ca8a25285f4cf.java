package com.coding.challenge.greedy;

class MinFunctionCallsToMakeTargetArray {
	MinFunctionCallsToMakeTargetArray() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 1, 5 };
		assert minOperations(nums1) == 5;

		final int[] nums2 = { 2, 2 };
		assert minOperations(nums2) == 3;

		final int[] nums3 = { 4, 2, 5 };
		assert minOperations(nums3) == 6;

		final int[] nums4 = { 0 };
		assert minOperations(nums4) == 0;
	}

	static int minOperations(int[] nums) {
		final int n = nums.length;
		int c = 0;
		// find the largest number first.
		int curr = 0;
		for (int i = 0; i < n; i++)
			if (curr < nums[i])
				curr = nums[i];

		// compute the number of function calls required by this number.
		while (curr > 0) {
			if (curr % 2 == 0) {
				curr = curr / 2;
				c = c + 1;
			} else
				curr = curr - 1;
		}

		for (int num : nums) {
			while (num > 0) {
				if (num % 2 == 0)
					num = num / 2;
				else {
					num = num - 1;
					c = c + 1;
				}
			}
		}
		return c;
	}
}