class Solution:
    def coinChange(self, coins: List[int], amount: int) -> int:
        min_coins = [float('inf')] * (amount + 1)
        min_coins[0] = 0
        coins = set(coins)
        for i in range(1, amount + 1):
            for c in coins:
                if i - c >= 0:
                    min_coins[i] = min(min_coins[i], 1 + min_coins[i - c])
        
        return min_coins[-1] if min_coins[-1] != float('inf') else -1
            