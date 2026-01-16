package com.coding.challenge.prefix.sum;

import java.util.HashMap;
import java.util.Map;

class ContiguousArray {
	ContiguousArray() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 0, 1 };
		assert findMaxLength(nums1) == 2;

		final int[] nums2 = { 0, 1, 0 };
		assert findMaxLength(nums2) == 2;
	}

	static int findMaxLength(int[] nums) {
		final int n = nums.length;
		final Map<Integer, Integer> scoreToPosition = new HashMap<>();
		// sentinel value.
		scoreToPosition.put(0, -1);
		int l = 0;
		for (int i = 0, score = 0; i < n; i++) {
			// consider 0 as -1.
			if (nums[i] == 0)
				score = score - 1;
			else
				score = score + 1;

			// solution found.
			if (scoreToPosition.containsKey(score))
				l = Math.max(l, i - scoreToPosition.get(score));
			else
				// add the score if it is absent. Otherwise, keep the old value.
				scoreToPosition.put(score, i);
		}
		return l;
	}
}