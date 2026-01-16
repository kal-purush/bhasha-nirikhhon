"""
50. Pow(x,n)

Implement pow(x,n), which calculates x raised to the power n (i.e., x**n)
"""

"""
Example 1:
Input: x = 2.00000, n = 10
Output: 1024.00000

Example 2:
Input: x = 2.10000, n = 3
Output: 9.26100

Example 3:
Input: x = 2.00000, n = -2
Output: 0.25000

Explanation: 2-2 = 1/22 = 1/4 = 0.25
"""

class Solution:
    def myPow(self, x: float, n: int) -> float:
        
        if n == 0:
            return 1.0
        
        abs_n = abs(n)

        total = x
        for _ in range(1, abs_n):
            total *= x
                
        if n < 0:
            total = 1/total
        
        return total

class Solution:
    def myPow(self, x: float, n: int) -> float:

        if n == 0:
            return 1.0

        negative = n < 0
        if negative:
            n = -n

        result = 1.0
        while n > 0:
            # if current bit is odd, multiply result by x
            if n % 2 == 1:
                result *= x

            # square x for the next bit
            x = x*x

            # move on to the next bit by halving n
            n = n//2
        
        if negative:
            result = 1.0/result

        return result