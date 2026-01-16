package com.coding.challenge.backtracking;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class NonDecreasingSubsequences {
	final Set<List<Integer>> s = new HashSet<>();

	NonDecreasingSubsequences() {
	}

	public static void main(String[] args) {
		final int[] nums1 = { 4, 6, 7, 7 };
		System.out.println(new NonDecreasingSubsequences().findSubsequences(nums1));

		final int[] nums2 = { 4, 4, 3, 2, 1, 5 };
		System.out.println(new NonDecreasingSubsequences().findSubsequences(nums2));
	}

	List<List<Integer>> findSubsequences(int[] nums) {
		backtracking(nums, 0, new ArrayList<>());
		final List<List<Integer>> ans = new ArrayList<>();
		for (List<Integer> increasingSubseq : s)
			ans.add(increasingSubseq);
		return ans;
	}

	private void backtracking(int[] nums, int i, List<Integer> currState) {
		if (i == nums.length) {
			if (currState.size() >= 2)
				s.add(new ArrayList<>(currState));
			return;
		}
		// exclude the current element and explore.
		backtracking(nums, i + 1, currState);
		// include the current element and explore.
		// is it increasing subsequence after appending the ith element.
		if (currState.isEmpty() || nums[i] >= currState.get(currState.size() - 1)) {
			// choose it.
			currState.add(nums[i]);
			// explore.
			backtracking(nums, i + 1, currState);
			// unchoose it.
			currState.remove(currState.size() - 1);
		}
	}
}