package com.coding.challenge.backtracking;

import java.util.ArrayList;
import java.util.List;

class CombinationSum {
	CombinationSum() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] candidates1 = { 2, 3, 6, 7 };
		System.out.println(combinationSum(candidates1, 7));

		final int[] candidates2 = { 2, 3, 5 };
		System.out.println(combinationSum(candidates2, 8));

		final int[] candidates3 = { 2 };
		System.out.println(combinationSum(candidates3, 1));
	}

	static List<List<Integer>> combinationSum(int[] candidates, int target) {
		final List<List<Integer>> cmbs = new ArrayList<>();
		backtrack(candidates, target, cmbs, new ArrayList<>(), 0);
		return cmbs;
	}

	private static void backtrack(int[] candidates, int target, List<List<Integer>> combinations, List<Integer> currCmb,
			int i) {
		// trivial case of the recursion.
		if (target <= 0) {
			// answer found.
			if (target == 0)
				combinations.add(new ArrayList<>(currCmb));
			return;
		}
		// recursive step.
		final int n = candidates.length;
		for (int j = i; j < n; j++) {
			// choose.
			currCmb.add(candidates[j]);
			// explore
			backtrack(candidates, target - candidates[j], combinations, currCmb, j);
			// unchoose
			currCmb.remove(currCmb.size() - 1);
		}
	}
}