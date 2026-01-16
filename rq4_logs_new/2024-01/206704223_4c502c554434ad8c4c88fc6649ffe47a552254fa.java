package com.coding.challenge.sorting;

import java.util.Arrays;

class MaximumConsecutiveFloorsWithoutSpecialFloors {
	MaximumConsecutiveFloorsWithoutSpecialFloors() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] special1 = { 4, 6 };
		assert maxConsecutive(2, 9, special1) == 3;

		final int[] special2 = { 7, 6, 8 };
		assert maxConsecutive(6, 8, special2) == 0;
	}

	static int maxConsecutive(int bottom, int top, int[] special) {
		final int n = special.length;
		Arrays.sort(special);
		int f = special[0] - bottom;
		for (int i = 1; i < n; i++)
			f = Math.max(f, special[i] - special[i - 1] - 1);
		f = Math.max(f, top - special[n - 1]);
		return f;
	}
}