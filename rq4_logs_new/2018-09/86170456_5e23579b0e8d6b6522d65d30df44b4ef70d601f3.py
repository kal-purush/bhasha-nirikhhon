class Solution(object):
    def longestPalindrome(self, s):
        """
        :type s: str
        :rtype: int
        """
        d = {}
        for i in s:
            if i in d:
                d[i] += 1
            else:
                d[i] = 1
        
        ans = 0
        odd = 0
        for k,v in d.items():
            ans += v
            if v % 2 == 1:
                odd += 1

        ans -= max(0, odd - 1)
        return ans
            
            
                    