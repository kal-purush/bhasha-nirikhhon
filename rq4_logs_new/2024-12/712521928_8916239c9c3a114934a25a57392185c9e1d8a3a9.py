"""
43. Multiply Strings

Given two non-negative integers `num1` and `num2` represented as strings,
return the product of `num1` and `num2`, also represented as a string.

Note: You must not use any built-in BigInteger library or convert the
inputs to integer directly.
"""

"""
Example 1:
Input: num1 = "2", num2 = "3"
Output: "6"

Example 2:
Input: num1 = "123", num2 = "456"
Output: "56088"
"""

class Solution:
    def multiply(self, num1: str, num2: str) -> str:
        if num1 == '0' or num2 == '0':
            return '0'
        
        m, n = len(num1), len(num2)
        results = [0] * (m+n)
        
        num1 = num1[::-1]
        num2 = num2[::-1]
        
        for i in range(m):
            for j in range(n):
                mult = int(num1[i])*int(num2[j])
                results[i+j] += mult
                results[i+j+1] += results[i+j] // 10
                results[i+j] %= 10
        
        while len(results) > 1 and results[-1] == 0:
            results.pop()

        results = results[::-1]
        return ''.join(map(str, results))

class Solution:
    def multiply(self, num1: str, num2: str) -> str:
        # Edge Case: If either number is '0', return '0'
        if num1 == '0' or num2 == '0':
            return '0'

        m, n = len(num1), len(num2)
        # Initialize result array
        result = [0] * (m + n)

        # Perform multiplication without reversing strings
        for i in range(m - 1, -1, -1):
            digit1 = int(num1[i])
            for j in range(n - 1, -1, -1):
                digit2 = int(num2[j])
                # Multiply current digits
                mult = digit1 * digit2
                # Positions in the result array
                p1, p2 = i + j, i + j + 1
                # Sum with the existing value
                total = mult + result[p2]
                # Update positions
                result[p2] = total % 10
                result[p1] += total // 10

        # Skip leading zeros
        start = 0
        while start < len(result) and result[start] == 0:
            start += 1

        # Convert result to string
        result_str = ''.join(map(str, result[start:]))
        return result_str if result_str else '0'