#
# [157] Read N Characters Given Read4
#
# https://leetcode.com/problems/read-n-characters-given-read4
#
# algorithms
# Easy (28.88%)
# Total Accepted:    34.3K
# Total Submissions: 118.7K
# Testcase Example:  '""\n0'
#
# 
# The API: int read4(char *buf) reads 4 characters at a time from a file.
# 
# 
# 
# The return value is the actual number of characters read. For example, it
# returns 3 if there is only 3 characters left in the file.
# 
# 
# 
# By using the read4 API, implement the function int read(char *buf, int n)
# that reads n characters from the file.
# 
# 
# 
# Note:
# The read function will only be called once for each test case.
# 
#
# The read4 API is already defined for you.
# @param buf, a list of characters
# @return an integer
# def read4(buf):

class Solution(object):
    def read(self, buf, n):
        """
        :type buf: Destination buffer (List[str])
        :type n: Maximum number of characters to read (int)
        :rtype: The number of characters read (int)
        """
        eof = False
        temp = [''] * 4
        total = 0
        while not eof and total < n:
            c = read4(temp)
            eof = c < 4
            for i in range(min(c, n - total)):
                buf[total] = temp[i]
                total += 1
        return total