package com.coding.challenge.dp;

class BestTimeToBuyAndSellStockIII {
	BestTimeToBuyAndSellStockIII() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] prices1 = { 3, 3, 5, 0, 0, 3, 1, 4 };
		assert maxProfit(prices1) == 6;

		final int[] prices2 = { 1, 2, 3, 4, 5 };
		assert maxProfit(prices2) == 4;

		final int[] prices3 = { 7, 6, 4, 3, 1 };
		assert maxProfit(prices3) == 0;
	}

	static int maxProfit(int[] prices) {
		final int n = prices.length;
		final int[] t2 = new int[n + 1];
		for (int i = n - 2, maxAfter = prices[n - 1]; i >= 0; i--) {
			t2[i] = Math.max(t2[i + 1], maxAfter - prices[i]);
			maxAfter = Math.max(maxAfter, prices[i]);
		}

		int maxProfit = 0;
		for (int i = 0, minBefore = prices[0], t1 = 0; i < n; i++) {
			t1 = Math.max(t1, prices[i] - minBefore);
			final int p = Math.max(t1, t1 + t2[i + 1]);
			maxProfit = Math.max(maxProfit, p);
			minBefore = Math.min(minBefore, prices[i]);
		}
		return maxProfit;
	}
}