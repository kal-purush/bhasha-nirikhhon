#coding=utf-8

import traceback, sys
from datetime import datetime, timedelta

class TimeFlowControl(object):
    '''
    记录一段时间内流量
    '''

    def __init__(self, unit, time_range):
        self.unit = unit
        self.time_range = time_range
        self.capacity = time_range/unit
        self.data = [0 for _ in xrange(self.capacity)]
        self.head = 0
        self.count = 0
        self.head_time = datetime.now()

    def add(self):
        now = datetime.now()
        delta = int((now - self.head_time).total_seconds()/self.unit)
        if delta < self.capacity:
            self.data[(self.head + delta) % self.capacity] += 1
            self.count += 1
        elif delta >= self.capacity and delta < 2*self.capacity:
            for _ in xrange(delta - self.capacity + 1):
                self.count -= self.data[self.head]
                self.data[self.head] = 0
                self.head = (self.head + 1) % self.capacity
            self.head_time += timedelta(seconds=(delta-self.capacity+1))
            self.data[(self.head - 1) % self.capacity] += 1
            self.count += 1
        else:
            for i in xrange(self.capacity):
                self.data[i] = 0
            self.head = 0
            self.head_time = now
            self.data[0] = 1
            self.count = 1

    def last_n_count(self, time_range):
        total = self.count
        for i in xrange(int((self.time_range - time_range)/self.unit)):
            total -= self.data[(self.head + i) % self.capacity]
        return total

if __name__ == '__main__':
    control = TimeFlowControl(1, 3)
    control.add()
    control.last_n_count(20)
    import pdb;pdb.set_trace()