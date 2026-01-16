package com.coding.challenge.dp;

import java.util.Arrays;

class MaximumLengthOfPairChain {
	MaximumLengthOfPairChain() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] pairs1 = { { 1, 2 }, { 2, 3 }, { 3, 4 } };
		assert findLongestChain(pairs1) == 2;

		final int[][] pairs2 = { { 1, 2 }, { 7, 8 }, { 4, 5 } };
		assert findLongestChain(pairs2) == 3;
	}

	static int findLongestChain(int[][] pairs) {
		final int n = pairs.length;
		final int[] chainLength = new int[n];
		// sort pairs by finishing time.
		Arrays.sort(pairs, (a, b) -> Integer.compare(a[1], b[1]));
		int longestChainLength = 1;

		for (int i = 0; i < n; i++) {
			// solution when we consider the pair alone.
			chainLength[i] = 1;
			// using prefixes.
			for (int j = 0; pairs[j][1] < pairs[i][0]; j++)
				chainLength[i] = Math.max(chainLength[i], chainLength[j] + 1);
			longestChainLength = Math.max(longestChainLength, chainLength[i]);
		}
		return longestChainLength;
	}
}