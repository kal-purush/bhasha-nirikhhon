package com.coding.challenge.binary.search;

class GuessNumberHigherOrLower {
	GuessNumberHigherOrLower() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		System.out.println(guessNumber(10));
	}

	static int guessNumber(int n) {
		int left = 1;
		int right = n;
		while (left <= right) {
			final int mid = (int) (((long)left + right) / 2);
			final int cmp = guess(mid);
			if (cmp == -1)
				right = mid - 1;	
			else if (cmp == 1)
				left = mid + 1;
			else
				return mid;
		}
		throw new AssertionError();
	}

	private static int guess(int num) {
		if (num < 6)
			return -1;
		if (num > 6)
			return 1;
		return 0;
	}
}