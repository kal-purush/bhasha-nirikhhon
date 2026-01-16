__author__ = 'jtakwani'
# Definition for an interval.

class Interval:
    def __init__(self, s=0, e=0):
        self.start = s
        self.end = e


class Solution:
    # @param intervals, a list of Intervals
    # @return a list of Interval
    def merge(self, intervals):

        intervals.sort(key=lambda interval: interval.start)
        retIntervals = []
        first = intervals[0].start
        last = intervals[0].end
        for i in range(1,len(intervals)):
            if last > intervals[i].end:
                #current interval overlaps new one
                continue
            elif last < intervals[i].start:
                #current interval ended
                retIntervals.append(Interval(first,last))
                first = intervals[i].start
                last = intervals[i].end
            else:
                # New interval last detected
                last = intervals[i].end
        retIntervals.append(Interval(first,last))
        print retIntervals



s = Solution()
s.merge([[1,3],[2,6],[8,10]])