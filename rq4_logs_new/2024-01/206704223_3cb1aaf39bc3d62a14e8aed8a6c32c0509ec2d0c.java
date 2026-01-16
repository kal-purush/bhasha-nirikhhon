package com.coding.challenge.backtracking;

import java.util.ArrayList;
import java.util.List;

class Subsets {
	Subsets() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 1, 2, 3 };
		System.out.println(subsets(nums1));
	}

	static List<List<Integer>> subsets(int[] nums) {
		final List<List<Integer>> ans = new ArrayList<>();
		backtrack(nums, ans, new ArrayList<>(), nums.length);
		return ans;
	}

	private static void backtrack(int[] nums, List<List<Integer>> powerSet, List<Integer> currSet, int n) {
		// trivial case of the recursion.
		if (n == 0) {
			powerSet.add(new ArrayList<Integer>(currSet));
			return;
		}

		// exclude the number and explore.
		backtrack(nums, powerSet, currSet, n - 1);
		// choose.
		// include the number and explore.
		currSet.add(nums[n - 1]);
		backtrack(nums, powerSet, currSet, n - 1);
		// unchoose.
		currSet.remove(currSet.size() - 1);
	}
}