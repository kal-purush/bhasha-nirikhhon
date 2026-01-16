package com.coding.challenge.sorting;

import java.util.Arrays;
import java.util.Comparator;

class RemoveCoveredIntervals {
	RemoveCoveredIntervals() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] intervals1 = { { 1, 4 }, { 3, 6 }, { 2, 8 } };
		assert removeCoveredIntervals(intervals1) == 2;

		final int[][] intervals2 = { { 1, 4 }, { 2, 3 } };
		assert removeCoveredIntervals(intervals2) == 1;

		final int[][] intervals3 = { { 1, 2 }, { 1, 4 }, { 3, 4 } };
		assert removeCoveredIntervals(intervals3) == 1;
	}

	static int removeCoveredIntervals(int[][] intervals) {
		final int n = intervals.length;
		/*
		 * sort intervals using start time, breaking ties using the reverse order of
		 * finishing time.
		 */
		final Comparator<int[]> cmp = new Comparator<int[]>() {
			@Override
			public int compare(int[] o1, int[] o2) {
				final int comparisonOutcome = Integer.compare(o1[0], o2[0]);
				return comparisonOutcome == 0 ? Integer.compare(o2[1], o1[1]) : comparisonOutcome;
			}
		};
		Arrays.sort(intervals, cmp);
		int remainingIntervals = n;
		for (int i = 0, f = 0; i < n; i++) {
			if (intervals[i][1] <= f)
				remainingIntervals = remainingIntervals - 1;
			f = Math.max(f, intervals[i][1]);
		}
		return remainingIntervals;
	}
}