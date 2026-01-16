#!/usr/bin/env python
# Reverse Integer https://oj.leetcode.com/problems/reverse-integer/
# Reverse digits of an integer.
# Example1: x = 123, return 321
# Example2: x = -123, return -321

#Math

# Xilin SUN
# Dec 7 2014

# I guess this is the ugly way to do it

class Solution:
	# @return an integer
	def reverse(self, x):
		if x > 2147483646:
			return 0
		if x < -2147483647:
			return 0
		isPositive = True
		if x < 0:
			isPositive = False
			x = -x
		rev = 0
		while x != 0:
			rev = 10 * rev + x % 10
			x = x / 10
		if rev > 2147483646:
			return 0
		if rev < -2147483647:
			return 0
		if isPositive:
			return rev
		return -rev