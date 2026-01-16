"""
-*- coding: utf-8 -*-
@file : NumArray.py
@description：区域和检索
@author : 杨睿
@time : 2021-03-02 14:43
"""
from typing import *


class NumArray:

    def __init__(self, nums: List[int]):
        n = len(nums)
        self.sums = [0] * (n+1)
        for i in range(n):
            self.sums[i+1] = self.sums[i] + nums[i]

    def sumRange(self, i: int, j: int) -> int:
        return self.sums[j+1] - self.sums[i]
