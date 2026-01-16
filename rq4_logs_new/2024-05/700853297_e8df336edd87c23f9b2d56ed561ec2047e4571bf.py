# Author: Runar Fosse
# Time complexity: O(n)
# Space complexity: O(1)

class Solution:
    def equalSubstring(self, s: str, t: str, maxCost: int) -> int:
        # Using sliding window
        n = len(s)

        max_len = 0
        p1, p2 = 0, 0
        while p2 < n:
            # Expand window
            maxCost -= abs(ord(s[p2]) - ord(t[p2]))
            p2 += 1

            # If we are over maxCost, shrink window
            if maxCost < 0:
                maxCost += abs(ord(s[p1]) - ord(t[p1]))
                p1 += 1
            
            # Store current longest substring that can be changed
            max_len = max(p2 - p1, max_len)
        
        return max_len