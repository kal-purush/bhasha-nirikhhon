"""
44. Wildcard Matching

Given an input string (s) and a pattern (p), implement wildcard pattern
matching with support for '?' and '*' where:

- '?' matches any single character.
- '*' matches any sequence of characters (including the empty sequence).

The matching should cover the entire input string (not partial).
"""

"""
Example 1:
Input: s = "aa", p = "a"
Output: false

Explanation: "a" does not match the entire string "aa".

Example 2:
Input: s = "aa", p = "*"
Output: true

Explanation: '*' matches any sequence.

Example 3:
Input: s = "cb", p = "?a"
Output: false

Explanation: '?' matches 'c', but the second letter is 'a', which does not match 'b'.
"""

class Solution:
    def isMatch(self, s: str, p: str) -> bool:
        
        m, n = len(s), len(p)
        # Initialize DP
        dp = [[False]*(n+1) for _ in range(m+1)]
        # Base Case - Empty String matches Empty Pattern
        dp[0][0] = True
        
        # Base Case - First Row:
        for j in range(1, n+1):
            if p[j-1] == '*':
                dp[0][j] = dp[0][j-1]
        
        # Filling Rest of the Table:
        for i in range(1, m+1):
            for j in range(1, n+1):
                # First scenario - *:
                if p[j-1] == '*':
                    dp[i][j] = dp[i-1][j] or dp[i][j-1]
                elif p[j-1] == '?' or p[j-1] == s[j-1]:
                    dp[i][j] = dp[i-1][j-1]
                else:
                    dp[i][j] = False
        
        return dp[m][n]