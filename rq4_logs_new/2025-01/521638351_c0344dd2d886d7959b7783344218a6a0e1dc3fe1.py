from typing import List

class Solution:
    def letterCombinations(self, digits: str) -> List[str]:
        if not digits:
            return []

        res = []
        digitsToChar = {
            '2': ['a', 'b', 'c'],
            '3': ['d', 'e', 'f'],
            '4': ['g', 'h', 'i'],
            '5': ['j', 'k', 'l'],
            '6': ['m', 'n', 'o'],
            '7': ['p', 'q', 'r', 's'],
            '8': ['t', 'u', 'v'],
            '9': ['w', 'x', 'y', 'z']
        }

        def dfs(i, s):
            if i >= len(digits):
                res.append("".join(s))
                return

            for ch in digitsToChar[digits[i]]:
                dfs(i + 1, s + [ch])

        dfs(0, [])
        return res

# TC: O(4^n) -> 4 is the maximum number of characters a digit can have
# SC: O(4^ n + n*4*n) -> O(4^n) for the recursion stack and O(n*4*n) for the result list