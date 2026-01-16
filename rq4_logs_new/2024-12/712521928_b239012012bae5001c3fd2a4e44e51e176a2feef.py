"""
57. Insert Interval

You are given an array of non-overlapping intervals `intervals` where intervals[i] = [start, end]
represent the start and the end of the i-th interval and intervals is sorted in ascending order by
start. You are also given an interval newInterval = [start, end] that represents the start and end
of another interval.

Insert newInterval into intervals such that intervals is still sorted in ascending order by start and
intervals still does not have any overlapping intervals (merge overlapping intervals if necessary).

Return intervals after the insertion.

Note that you don't need to modify intervals in-place. You can make a new array and return it.
"""

"""
Example 1:
Input: intervals = [[1,3],[6,9]], newInterval = [2,5]
Output: [[1,5],[6,9]]

Example 2:
Input: intervals = [[1,2],[3,5],[6,7],[8,10],[12,16]], newInterval = [4,8]
Output: [[1,2],[3,10],[12,16]]

Explanation: Because the new interval [4,8] overlaps with [3,5],[6,7],[8,10].
"""

class Solution:
    def insert(self, intervals: list[list[int]], newInterval = list[int]) -> list[list[int]]:
        intervals.append(newInterval)
        
        #sort
        intervals.sort(key=lambda x: x[0])
        merged = []
        
        for interval in intervals:
            if not merged or merged[-1][1] < interval[0]:
                merged.append(interval)
            else:
                merged[-1][1] = max(merged[-1][1], interval[1])

        return merged

class Solution:
    def insert(self, intervals: list[list[int]], newInterval = list[int]) -> list[list[int]]:

        merged = []
        i = 0
        n = len(intervals)

        # No Overlap Case
        while i < n and intervals[i][1] < newInterval[0]:
            merged.append(intervals[i])
            i += 1

        # Overlap Case
        editedInterval = newInterval
        #as long as next interval's start is less than or equal to current merged interval's end - overlap
        while i < n and intervals[i][0] <= editedInterval[1]:
            editedInterval[0] = min(intervals[i][0], editedInterval[0])
            editedInterval[1] = max(intervals[i][1], editedInterval[1])
            i += 1

        merged.append(editedInterval)

        while i < n:
            merged.append(intervals[i])
            i += 1
        
        return merged