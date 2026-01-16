package com.coding.challenge.prefix.sum;

class MinimumPenaltyForShop {
	private static final char Y = 'Y';
	private static final char N = 'N';

	MinimumPenaltyForShop() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		assert bestClosingTime("YYNY") == 2;
		assert bestClosingTime("NNNNN") == 0;
		assert bestClosingTime("YYYY") == 4;
		assert bestClosingTime("N") == 0;
		assert bestClosingTime("YNYY") == 4;
	}

	static int bestClosingTime(String customers) {
		final int n = customers.length();
		final int[] preNcnt = new int[n + 1];
		for (int i = 1; i <= n; i++)
			preNcnt[i] = customers.charAt(i - 1) == N ? preNcnt[i - 1] + 1 : preNcnt[i - 1];

		final int[] postYcnt = new int[n + 2];
		for (int j = n; j > 0; j--)
			postYcnt[j] = customers.charAt(j - 1) == Y ? postYcnt[j + 1] + 1 : postYcnt[j + 1];

		int h = 0;
		for (int i = 0; i <= n; i++)
			if (preNcnt[h] + postYcnt[h + 1] > preNcnt[i] + postYcnt[i + 1])
				h = i;

		return h;
	}
}