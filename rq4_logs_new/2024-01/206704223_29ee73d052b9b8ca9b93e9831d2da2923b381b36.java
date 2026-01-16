package com.coding.challenge.array;

class FindClosestNumberToZero {
	FindClosestNumberToZero() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { -4, -2, 1, 4, 8 };
		assert findClosestNumber(nums1) == 1;

		final int[] nums2 = { 2, -1, 1 };
		assert findClosestNumber(nums2) == 1;
	}

	static int findClosestNumber(int[] nums) {
		final int n = nums.length;
		int idx = 0;
		for (int i = 0; i < n; i++) {
			if (Math.abs(nums[i]) < Math.abs(nums[idx]))
				idx = i;
			else if (Math.abs(nums[i]) == Math.abs(nums[idx]) && nums[i] > nums[idx])
				idx = i;
		}
		return nums[idx];
	}
}