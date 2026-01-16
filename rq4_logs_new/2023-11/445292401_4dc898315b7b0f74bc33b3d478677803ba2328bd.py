class Solution:
    def coinChange(self, coins: List[int], amount: int) -> int:
        return self.memoization(coins, amount, {})

    def memoization(self, coins: List[int], amount: int, memory: dict[int, int]) -> int:
        if amount == 0:
            return 0

        if amount in memory:
            return memory[amount]

        options = []
        for c in coins:
            options.append(self.memoization(coins, amount - c, memory))

        memory[amount] = 1 + min(options)
        return memory[amount]

'''
coinChange([1,2,5], 11)
min(coinChange([1,2,5], 10), coinChange([1,2,5], 9), coinChange([1,2,5], 6))

coinChange([1,2,5], 10)
min(coinChange([1,2,5], 9), coinChange([1,2,5], 7), coinChange([1,2,5], 5))

coinChange([1,2,5], 5)
min(coinChange([1,2,5], 4), coinChange([1,2,5], 3), coinChange([1,2,5], 0))

11
[10, 9, 6]

[]
'''