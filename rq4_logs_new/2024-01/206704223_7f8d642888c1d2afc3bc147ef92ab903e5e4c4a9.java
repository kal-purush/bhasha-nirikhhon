package com.coding.challenge.backtracking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class SubsetsII {
	final Set<List<Integer>> subSets = new HashSet<>();

	SubsetsII() {
	}

	public static void main(String[] args) {
		final int[] nums1 = { 1, 2, 2 };
		System.out.println(new SubsetsII().subsetsWithDup(nums1));
		
		final int[] nums2 = { 1, 2, 2, 1 };
		System.out.println(new SubsetsII().subsetsWithDup(nums2));
	}

	List<List<Integer>> subsetsWithDup(int[] nums) {
		Arrays.sort(nums);
		backtrack(nums, 0, new ArrayList<>());
		final List<List<Integer>> ans = new ArrayList<>();
		for (List<Integer> list : subSets)
			ans.add(list);

		return ans;
	}

	private void backtrack(int[] nums, int currChoice, List<Integer> choices) {
		// trivial case of the recursion.
		if (currChoice == nums.length) {
			subSets.add(new ArrayList<>(choices));
			return;
		}

		// exclude it and explore.
		backtrack(nums, currChoice + 1, choices);
		// choose.
		choices.add(nums[currChoice]);
		// include it and explore.
		backtrack(nums, currChoice + 1, choices);
		// unchoose.
		choices.remove(choices.size() - 1);
	}
}