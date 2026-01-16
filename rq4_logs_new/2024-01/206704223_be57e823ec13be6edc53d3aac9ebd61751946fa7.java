package com.coding.challenge.dp;

class StoneGame {
	StoneGame() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] piles1 = { 5, 3, 4, 5 };
		assert stoneGame(piles1);

		final int[] piles2 = { 3, 7, 2, 3 };
		assert stoneGame(piles2);

		final int[] piles3 = { 2, 4, 1, 6, 5, 3 };
		assert stoneGame(piles3);

		final int[] piles4 = { 3, 2, 10, 4 };
		assert stoneGame(piles4);
	}

	static boolean stoneGame(int[] piles) {
		final int n = piles.length;
		final int[][] score = new int[n][n];
		final int[][] sum = new int[n][n];
		// trivial case of the recursion.
		for (int i = 0; i < n; i++) {
			score[i][i] = piles[i];
			sum[i][i] = piles[i];
		}

		// non-trivial case of the recursion.
		for (int l = 2; l <= n; l++) {
			for (int i = 0; i < n - l + 1; i++) {
				final int j = i + l - 1;
				// sum is associative and commutative operator.
				sum[i][j] = sum[i][j - 1] + piles[j];
				// we have two choices i and j, consider each. local brute force.
				final int choice1 = piles[i] + sum[i + 1][j] - score[i + 1][j];
				final int choice2 = piles[j] + sum[i][j - 1] - score[i][j - 1];
				score[i][j] = Math.max(choice1, choice2);
			}
		}
		return score[0][n - 1] > sum[0][n - 1] - score[0][n - 1];
	}
}