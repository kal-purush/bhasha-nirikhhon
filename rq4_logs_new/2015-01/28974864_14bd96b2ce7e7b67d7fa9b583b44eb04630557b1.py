# Remove Element https://oj.leetcode.com/problems/remove-element/
# Given an array and a value, remove all instances of that value in place and return the new length.
# The order of elements can be changed. It doesn't matter what you leave beyond the new length.

#Arrays

# Xilin SUN
# Jan 8 2015

# Don't forget to return 0 if A == [].

class Solution:
    # @param    A       a list of integers
    # @param    elem    an integer, value need to be removed
    # @return an integer
    def removeElement(self, A, elem):
        if len(A) == 0:
    	    return 0
        else:
            k=0
            for i in range(0, len(A)):
                if A[i] != elem:
                    A[k] = A[i]
                    k += 1
            return k