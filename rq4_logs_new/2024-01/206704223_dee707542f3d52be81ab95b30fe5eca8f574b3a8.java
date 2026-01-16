package com.coding.challenge.hash.table;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class IntervalsBetweenIdenticalElements {
	IntervalsBetweenIdenticalElements() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] arr1 = { 2, 1, 3, 1, 2, 3, 3 };
		final long[] a1 = { 4, 2, 7, 2, 4, 4, 5 };
		assert Arrays.equals(a1, getDistances(arr1));

		final int[] arr2 = { 10, 5, 10, 10 };
		final long[] a2 = { 5, 0, 3, 4 };
		assert Arrays.equals(a2, getDistances(arr2));

		final int[] arr3 = { 1, 4 };
		final long[] a3 = { 0, 0 };
		assert Arrays.equals(a3, getDistances(arr3));
	}

	static long[] getDistances(int[] arr) {
		final int n = arr.length;
		final long[] intervals = new long[n];
		final Map<Integer, Integer> prefixFreq = new HashMap<>();
		final Map<Integer, Long> prefixDistance = new HashMap<>();
		final Map<Integer, Integer> prefixPreviousIndex = new HashMap<>();
		// prefix distance.
		for (int i = 0; i < n; i++) {
			if (prefixFreq.containsKey(arr[i])) {
				final int k = prefixPreviousIndex.get(arr[i]);
				intervals[i] = prefixDistance.getOrDefault(arr[i], 0l) + ((long) (i - k)) * prefixFreq.get(arr[i]);
				prefixDistance.put(arr[i], intervals[i]);
			}
			prefixFreq.merge(arr[i], 1, Integer::sum);
			prefixPreviousIndex.put(arr[i], i);
		}

		// now do the same for postfix distance
		final Map<Integer, Integer> postfixFreq = new HashMap<>();
		final Map<Integer, Long> postfixDistance = new HashMap<>();
		final Map<Integer, Integer> postfixPreviousIndex = new HashMap<>();
		for (int j = n - 1; j >= 0; j--) {
			if (postfixFreq.containsKey(arr[j])) {
				final int k = postfixPreviousIndex.get(arr[j]);
				final long distance = postfixDistance.getOrDefault(arr[j], 0l)
						+ ((long) (k - j)) * postfixFreq.get(arr[j]);
				intervals[j] = intervals[j] + distance;
				postfixDistance.put(arr[j], distance);
			}
			postfixFreq.merge(arr[j], 1, Integer::sum);
			postfixPreviousIndex.put(arr[j], j);
		}
		return intervals;
	}
}