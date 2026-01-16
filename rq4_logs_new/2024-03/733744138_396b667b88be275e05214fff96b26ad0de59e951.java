package br.dev.ferreiras.challenges.leetCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
You are given an array of non-overlapping intervals intervals where
intervals[i] = [starti, endi] represent the start and the end of the ith
interval and intervals is sorted in ascending order by starti.
You are also given an interval newInterval = [start, end] that represents
the start and end of another interval.

Insert newInterval into intervals such that intervals is still sorted in
ascending order by starti and intervals still does not have any overlapping
intervals (merge overlapping intervals if necessary).

Return intervals after the insertion.

Note that you don't need to modify intervals in-place.
You can make a new array and return it.

Example 1:

Input: intervals = [[1,3],[6,9]], newInterval = [2,5]
Output: [[1,5],[6,9]]
Example 2:

Input: intervals = [[1,2],[3,5],[6,7],[8,10],[12,16]], newInterval = [4,8]
Output: [[1,2],[3,10],[12,16]]
Explanation:
Because the new interval [4,8] overlaps with [3,5],[6,7],[8,10].


Constraints:

0 <= intervals.length <= 104
intervals[i].length == 2
0 <= starti <= endi <= 105
intervals is sorted by starti in ascending order.
newInterval.length == 2
0 <= start <= end <= 105

 */
public class InsertInterval {
  public static void main(String[] args) {
    int[][] intervals = {
            {1, 2},
            {3, 5},
            {6, 7},
            {8, 10},
            {12, 16}
    };
    int[] interval = {4,8};
    int[][] r = insert(intervals, interval);

    for (int[] s : r) {
      System.out.println(Arrays.toString(s));
    }
  }

  public static int[][] insert(int[][] intervals, int[] newInterval) {
    int insert = 0;
    int size = intervals.length;
    List<int[]> result = new ArrayList<>();

    while (insert < size) {

      if (intervals[insert][1] < newInterval[0])
        result.add(intervals[insert]);
      else if (intervals[insert][0] > newInterval[1]) {
        break;
      } else {
        // Overlap : merge them
        newInterval[0] = Math.min(newInterval[0], intervals[insert][0]);
        newInterval[1] = Math.max(newInterval[1], intervals[insert][1]);
      }
      insert++;

    }

    result.add(newInterval);
    while (insert < size) {
      result.add(intervals[insert++]);
    }

    return result.toArray(new int[result.size()][2]);

  }
}