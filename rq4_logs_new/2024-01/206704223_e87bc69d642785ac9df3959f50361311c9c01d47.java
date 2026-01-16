package com.coding.challenge.general;

class NumberOfPairsOfStringsWithConcatenationEqualToTarget {
	NumberOfPairsOfStringsWithConcatenationEqualToTarget() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final String[] nums1 = { "777", "7", "77", "77" };
		assert numOfPairs(nums1, "7777") == 4;

		final String[] nums2 = { "123", "4", "12", "34" };
		assert numOfPairs(nums2, "1234") == 2;

		final String[] nums3 = { "1", "1", "1" };
		assert numOfPairs(nums3, "11") == 6;
	}

	static int numOfPairs(String[] nums, String target) {
		final int n = nums.length;
		int c = 0;
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < i; j++) {
				final String candidate = nums[i] + nums[j];
				if (target.equals(candidate))
					c = c + 1;
			}

			for (int j = i + 1; j < n; j++) {
				final String candidate = nums[i] + nums[j];
				if (target.equals(candidate))
					c = c + 1;
			}
		}
		return c;
	}
}