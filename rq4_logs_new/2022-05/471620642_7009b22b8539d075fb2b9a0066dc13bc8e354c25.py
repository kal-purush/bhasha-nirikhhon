class Solution(object):
    def sumBase(self, n, k):
        """
        :type n: int
        :type k: int
        :rtype: int
        """
        converted = ''

        while n > 0:
            n, mod = divmod(n, k)
            converted += str(mod)

        return sum(map(int, converted))