package rish.leets.grind.medium;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/*
 * 
 * Problem #: 57
 * Problem link: https://leetcode.com/problems/insert-interval/
 * 
 * @author Rishabh Soni
 * 
 * Date Attempted: 12/06/2023
 *
 */
public class InsertInterval {

	public int[][] insert(int[][] intervals, int[] newInterval) {

		int size = intervals.length;
		if (size == 0) {
			return new int[][] { newInterval };
		}

		// Check if new interval is completely out of bounds
		if (intervals[0][0] > newInterval[1] || intervals[size - 1][1] < newInterval[0]) {

			boolean insertLeft = intervals[0][0] > newInterval[1];
			int inserIndex = insertLeft ? 0 : size;
			int offset = insertLeft ? 1 : 0;

			int[][] answer = new int[size + 1][2];
			answer[inserIndex] = newInterval;
			for (int i = 0; i < size; i++) {
				answer[i + offset] = intervals[i];
			}

			return answer;
		}

		List<List<Integer>> answer = new ArrayList<>();
		List<Integer> previousWindow = Collections.emptyList();
		boolean anyOverlappingFound = false;

		for (int i = 0; i < intervals.length; i++) {

			if (isOverlapping(intervals[i], newInterval)) {

				anyOverlappingFound = true;

				// find combined window with new interval
				int left = Math.min(intervals[i][0], newInterval[0]);
				int right = Math.max(intervals[i][1], newInterval[1]);

				List<Integer> newRange = Arrays.asList(left, right);

				if (!previousWindow.isEmpty() && isOverlapping(newRange.stream().mapToInt(Integer::intValue).toArray(),
						previousWindow.stream().mapToInt(Integer::intValue).toArray())) {

					int newLeft = Math.min(previousWindow.get(0), newRange.get(0));
					int newRight = Math.max(previousWindow.get(1), newRange.get(1));
					newRange = Arrays.asList(newLeft, newRight);

					if (newRange.equals(previousWindow)) {
						continue;
					}

					if (range(newRange) > range(previousWindow)) {
						answer.remove(previousWindow);
					}
				}

				previousWindow = newRange;
				answer.add(newRange);
			} else {
				previousWindow = Arrays.asList(intervals[i][0], intervals[i][1]);
				answer.add(Arrays.asList(intervals[i][0], intervals[i][1]));
			}
		}

		// add interval manually
		if (!anyOverlappingFound) {
			for (int i = 0; i < answer.size() - 1; i++) {
				if (answer.get(i).get(1) < newInterval[0] && newInterval[1] < answer.get(i + 1).get(0)) {
					answer.add(i + 1, Arrays.asList(newInterval[0], newInterval[1]));
				}
			}
		}

		return answer.stream().map(l -> l.stream().mapToInt(Integer::intValue).toArray()).toArray(int[][]::new);
	}

	private boolean isOverlapping(int[] source, int[] query) {
		return source[0] <= query[1] && source[1] >= query[0];
	}

	private int range(List<Integer> interval) {
		return interval.get(1) - interval.get(0);
	}

}