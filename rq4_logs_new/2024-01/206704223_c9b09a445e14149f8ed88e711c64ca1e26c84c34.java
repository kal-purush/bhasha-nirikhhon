package com.coding.challenge.sliding.window;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class Find_K_ClosestElements {
	Find_K_ClosestElements() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] arr1 = { 1, 2, 3, 4, 5 };
		assert Arrays.asList(1, 2, 3, 4).equals(findClosestElements(arr1, 4, 3));

		final int[] arr2 = { 1, 2, 3, 4, 5 };
		assert Arrays.asList(1, 2, 3, 4).equals(findClosestElements(arr2, 4, -1));

		final int[] arr3 = { 1, 1, 1, 10, 10, 10 };
		assert Arrays.asList(10).equals(findClosestElements(arr3, 1, 9));
	}

	static List<Integer> findClosestElements(int[] arr, int k, int x) {
		final int n = arr.length;
		int finish = 0;
		for (int i = 0, score = Integer.MAX_VALUE, currScore = 0; i < n; i++) {
			currScore = currScore + Math.abs(arr[i] - x);
			if (i >= k - 1) {
				if (currScore < score) {
					score = currScore;
					finish = i;
				}
				currScore = currScore - Math.abs(arr[i - k + 1] - x);
			}
		}
		final List<Integer> l = new ArrayList<>();
		for (int j = finish - k + 1; j <= finish; j++)
			l.add(arr[j]);

		return l;
	}
}