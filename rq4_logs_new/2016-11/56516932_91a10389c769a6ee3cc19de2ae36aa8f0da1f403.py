import math


class Solution(object):

    def isPalindrome(self, x):
        """
        :type x: int
        :rtype: bool
        """
        if x < 0:
            return False

        if x == 0:
            return True

        num_len = int(math.log10(x)) + 1
        if num_len == 1:
            return True

        l_idx = num_len
        r_idx = 0

        rslt = True

        while l_idx > r_idx:
            high_digit = x % (10**(l_idx)) / (10 ** (l_idx - 1))
            low_digit = x % (10 ** (r_idx + 1)) / (10 ** r_idx)

            if high_digit != low_digit:
                rslt = False
                break

            l_idx -= 1
            r_idx += 1

        return rslt


foo = Solution()
print(foo.isPalindrome(122) == False)
print(foo.isPalindrome(12321) == True)
print(foo.isPalindrome(-1) == False)
print(foo.isPalindrome(0) == True)
print(foo.isPalindrome(1) == True)