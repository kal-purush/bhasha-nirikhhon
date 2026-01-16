# DESCRIPTION
# Given a range [m, n] where 0 <= m <= n <= 2147483647,
# return the bitwise AND of all numbers in this range, inclusive.

# EXAMPLE 1:
# Input: [5,7]
# Output: 4

# EXAMPLE 2:
# Input: [0,1]
# Output: 0


class Solution:
    def rangeBitwiseAnd(self, m: int, n: int) -> int:
        '''
        Time: O(N), where N is the bit length of n
        Space: O(1), no data structure used
        '''

        # 0 & with anything is 0
        # also m <= n, so we only need to check that one
        if m == 0:
            return 0

        # count to know how many shifts are necessary to go back to
        i = 0

        while m != n:
            # drop the last bit
            m >>= 1
            n >>= 1
            # keep track of how many bits dropped
            i += 1
        # Multiplied by the i power of 2
        return m << i