package com.coding.challenge.sorting;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

class FindScoreOfArrayAfterMarkingAllElements {
	FindScoreOfArrayAfterMarkingAllElements() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 2, 1, 3, 4, 5, 2 };
		assert findScore(nums1) == 7;

		final int[] nums2 = { 2, 3, 5, 1, 3, 2 };
		assert findScore(nums2) == 5;
	}

	static long findScore(int[] nums) {
		final int n = nums.length;
		final Comparator<Integer> cmp = new Comparator<Integer>() {
			@Override
			public int compare(Integer i, Integer j) {
				final int comparison = Integer.compare(nums[i], nums[j]);
				if (comparison == 0)
					return Integer.compare(i, j);
				return comparison;
			}
		};
		final Integer[] orderedIndices = new Integer[n];
		for (int i = 0; i < n; i++)
			orderedIndices[i] = i;

		Arrays.sort(orderedIndices, cmp);
		final boolean[] marked = new boolean[n];
		long score = 0;
		for (int i = 0; i < n; i++) {
			final int k = orderedIndices[i];
			if (!marked[k]) {
				score = score + nums[k];
				marked[k] = true;
				if (k > 0)
					marked[k - 1] = true;
				if (k < n - 1)
					marked[k + 1] = true;
			}
		}
		return score;
	}
}