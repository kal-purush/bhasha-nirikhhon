package com.coding.challenge.prefix.sum;

import java.util.ArrayList;
import java.util.List;

class AllDivisionsWithHighestScoreOfBinaryArray {
	AllDivisionsWithHighestScoreOfBinaryArray() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 0, 0, 1, 0 };
		System.out.println(maxScoreIndices(nums1));

		final int[] nums2 = { 0, 0, 0 };
		System.out.println(maxScoreIndices(nums2));

		final int[] nums3 = { 1, 1 };
		System.out.println(maxScoreIndices(nums3));

		final int[] nums4 = { 1 };
		System.out.println(maxScoreIndices(nums4));

		final int[] nums5 = { 0 };
		System.out.println(maxScoreIndices(nums5));
	}

	static List<Integer> maxScoreIndices(int[] nums) {
		final int n = nums.length;
		final int[] prefixZeros = new int[n + 1];
		for (int i = 1; i <= n; i++)
			prefixZeros[i] = nums[i - 1] == 0 ? prefixZeros[i - 1] + 1 : prefixZeros[i - 1];

		int maxScore = prefixZeros[n];
		final int[] score = new int[n + 1];
		score[n] = maxScore;
		for (int j = n, postfixOnes = 0; j > 0; j--) {
			if (nums[j - 1] == 1)
				postfixOnes = postfixOnes + 1;
			final int currScore = prefixZeros[j - 1] + postfixOnes;
			score[j - 1] = currScore;
			maxScore = Math.max(maxScore, currScore);
		}
		final List<Integer> ans = new ArrayList<>();
		for (int i = 0; i <= n; i++)
			if (score[i] == maxScore)
				ans.add(i);
		return ans;
	}
}