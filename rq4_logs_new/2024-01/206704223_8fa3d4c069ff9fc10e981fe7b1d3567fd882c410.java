package com.coding.challenge.dp;

class BurstBalloons {
	BurstBalloons() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] balloons1 = { 3, 1, 5, 8 };
		assert maxCoins(balloons1) == 167;

		final int[] balloons2 = { 1, 5 };
		assert maxCoins(balloons2) == 10;
	}

	static int maxCoins(int[] nums) {
		final int n = nums.length;
		final int[][] c = new int[n + 2][n + 1];
		final int[] b = new int[n + 2];
		b[0] = 1;
		b[n + 1] = 1;
		for (int i = 0; i < n; i++)
			b[i + 1] = nums[i];

		for (int l = 1; l <= n; l++) {
			for (int i = 1; i <= n - l + 1; i++) {
				final int j = i + l - 1;
				for (int k = i; k <= j; k++)
					c[i][j] = Math.max(c[i][j], c[i][k - 1] + c[k + 1][j] + b[i - 1] * b[k] * b[j + 1]);
			}
		}
		return c[1][n];
	}
}