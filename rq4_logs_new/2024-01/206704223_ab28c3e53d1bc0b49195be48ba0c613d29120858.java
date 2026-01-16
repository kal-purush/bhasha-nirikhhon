package com.coding.challenge.dp;

class MinimumDeletionsToMakeStringBalanced {
	MinimumDeletionsToMakeStringBalanced() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		assert minimumDeletions("aababbab") == 2;
		assert minimumDeletions("bbaaaaabb") == 2;
	}

	static int minimumDeletions(String s) {
		final int n = s.length();
		int d = 0;
		for (int i = n - 1, aCnt = 0; i >= 0; i--) {
			if (s.charAt(i) == 'b')
				d = Math.min(d + 1, aCnt);
			else
				aCnt = aCnt + 1;
		}
		return d;
	}
}