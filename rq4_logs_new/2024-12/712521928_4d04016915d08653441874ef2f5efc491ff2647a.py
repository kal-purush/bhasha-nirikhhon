"""
60. Permutation Sequence

The set [1, 2, 3, ..., n] contains a total of n! unique permutations.

By listing and labelling all of the permutations in order, we get the following sequence for n = 3:

1. "123"
2. "132"
3. "213"
4. "231"
5. "312"
6. "321"

Given n and k, return the kth permutation sequence.
"""

"""
Example 1:
Input: n = 3, k = 3
Output: "213"

Example 2:
Input: n = 4, k = 9
Output: "2314"

Example 3:
Input: n = 3, k = 1
Output: "123"
"""

class Solution:
    def getPermutation(self, n: int, k: int) -> str:
        #1. Precompute Factorials
        factorials = [1] * (n+1)
        for i in range(1, n+1):
            factorials[i] = factorials[i-1]*i

        #2. Prepare list of available digits
        digits = [str(i) for i in range(1, n+1)]

        #3. Convert K to zero-based
        k -= 1

        #4. Build K-th permutation
        result = []
        for i in range(n, 0, -1):
            # Number of permutations for each initial digit (i-1)
            block_size = factorials[i-1]
            # Determine which digit should be at the current position
            index = k // block_size
            result.append(digits[index])

            # Remove used digits from list
            digits.pop(index)
            # Update k to the remainder for the next iteration
            k %= block_size

        return "".join(result)