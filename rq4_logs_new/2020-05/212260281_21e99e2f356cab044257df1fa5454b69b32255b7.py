# Say you have an array prices for which the ith element
# is the price of a given stock on day i.

# Design an algorithm to find the maximum profit.
# You may complete as many transactions as you like
# (i.e., buy one and sell one share of the stock multiple times).

# Note: You may not engage in multiple transactions at the same time
# (i.e., you must sell the stock before you buy again).

# EXAMPLE
# Input: [7,1,5,3,6,4]
# Output: 7
# Explanation: Buy on day 2 (price = 1) and sell on day 3 (price = 5), profit = 5-1 = 4.
#              Then buy on day 4 (price = 3) and sell on day 5 (price = 6), profit = 6-3 = 3.

class Solution:
    def maxProfit(self, prices: List[int]) -> int:
        '''
        Time: O(N), only need to iterate over the array once
        Space: O(1), constant space no extra data structures or stack frames
        '''

        """
        Suppose the first sequence is "a <= b <= c <= d",
        the profit is "d - a = (b - a) + (c - b) + (d - c)" without a doubt.
        And suppose another one is "a <= b >= b' <= c <= d",
        the profit is not difficult to be figured out as "(b - a) + (d - b')".
        So you just target at monotone sequences.
        """
        # Edge cases
        # if the array is empty or
        # array size is less than 1
        if not prices or len(prices) <= 1:
            return 0

        max_profit = 0

        # iterate over the array-1 for the index out of bounds
        for i in range(1,len(prices)):
            # if the current price is > the prev prices
            if prices[i] > prices[i-1]:
                # then profit can be made
                max_profit += prices[i] - prices[i-1]

        return max_profit