"""
56. Merge Intervals

Given an array of intervals where intervals[i] = [start, end], merge all overlapping intervals, and
return an array of the non-overlapping intervals that cover all the intervals in the input.
"""

"""
Example 1:
Input: intervals = [[1,3],[2,6],[8,10],[15,18]]
Output: [[1,6],[8,10],[15,18]]

Explanation: Since intervals [1,3] and [2,6] overlap, merge them into [1,6].

Example 2:
Input: intervals = [[1,4],[4,5]]
Output: [[1,5]]

Explanation: Intervals [1,4] and [4,5] are considered overlapping.
"""

class Solution:
    def merge(self, intervals: list[list[int]]) -> list[list[int]]:
        intervals.sort(lambda x: x[0])
        
        merged = []
        
        for i in intervals:
            #if merged is empty or current interval does not overlap with the last one in merged
            if not merged or merged[-1][1] < i[0]:
                merged.append(i)

            else:
                merged[-1][1] = max(merged[-1][1], i[1])
        
        return merged