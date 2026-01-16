package com.coding.challenge.binary.search;

import com.coding.challenge.BinarySearch;

class SearchInsertPosition {
	SearchInsertPosition() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums = { 1, 3, 5, 6 };
		assert searchInsert(nums, 5) == 2;

		assert searchInsert(nums, 2) == 1;

		assert searchInsert(nums, 7) == 4;
		assert searchInsert(nums, 0) == 0;
	}

	static int searchInsert(int[] nums, int target) {
		return BinarySearch.successor(nums, target - 1);
	}
}