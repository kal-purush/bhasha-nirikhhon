class Solution(object):
    def longestPalindrome(self, s):
        """
        :type s: str
        :rtype: str
        """
        palindrome = ""
        lens = len(s)
        for i in range(lens):
            pOneCenter = self.findLongestPalindrome(s, i, i)
            pTwoCenters = self.findLongestPalindrome(s, i, i+1)
    
            if (max(len(pOneCenter), len(pTwoCenters)) > len(palindrome)):   
                if (len(pOneCenter)>len(pTwoCenters)):
                    palindrome = pOneCenter
                else:
                    palindrome = pTwoCenters
        return palindrome
                
            
    def findLongestPalindrome(self,s,l,r):
        while (l>=0) and (r<len(s)) and (s[l]==s[r]):
            l -= 1
            r += 1
        return s[l+1:r]