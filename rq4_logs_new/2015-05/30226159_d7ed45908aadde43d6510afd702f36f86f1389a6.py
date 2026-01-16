#!/usr/local/env python
# -*- coding: utf-8 -*-

'''
Given an array with n objects colored red, white or blue, sort them so that objects of the same color are adjacent, with the colors in the order red, white and blue.

Here, we will use the integers 0, 1, and 2 to represent the color red, white, and blue respectively.

Note:
You are not suppose to use the library's sort function for this problem.
'''

class Solution:
    # @param A a list of integers
    # @return nothing, sort in place
    def sortColors(self, A):
        left = 0
        right = len(A)-1
        i = 0
        while i<=right:
#            print left, i, right, A
            if A[i]==0:
                A[left], A[i] = A[i], A[left]
                left += 1
                if left>=i:
                    i += 1
            elif A[i]==2:
                A[i], A[right] = A[right], A[i]
                right -= 1
            else:
                i += 1
        print A

a = "00021"
a = [ int(i) for i in list(a) ]
t = Solution()
print t.sortColors(a)
