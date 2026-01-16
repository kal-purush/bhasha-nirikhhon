package com.coding.challenge.dp;

public class PredictTheWinner {
	PredictTheWinner() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 1, 5, 2 };
		System.out.println(predictTheWinner(nums1));

		final int[] nums2 = { 1, 5, 233, 7 };
		System.out.println(predictTheWinner(nums2));
	}

	static boolean predictTheWinner(int[] nums) {
		final int n = nums.length;
		final int[][] score = new int[n][n];
		final int[][] sum = new int[n][n];
		// trivial case of the recursion.
		for (int i = 0; i < n; i++) {
			score[i][i] = nums[i];
			sum[i][i] = nums[i];
		}

		// non-trivial case of the recursion.
		for (int l = 2; l <= n; l++) {
			for (int i = 0; i < n - l + 1; i++) {
				final int j = i + l - 1;
				// sum is associative and commutative operator.
				sum[i][j] = sum[i][j - 1] + nums[j];
				// we have two choices i and j, consider each. local brute force.
				final int choice1 = nums[i] + sum[i + 1][j] - score[i + 1][j];
				final int choice2 = nums[j] + sum[i][j - 1] - score[i][j - 1];
				score[i][j] = Math.max(choice1, choice2);
			}
		}
		return score[0][n - 1] >= sum[0][n - 1] - score[0][n - 1];
	}
}