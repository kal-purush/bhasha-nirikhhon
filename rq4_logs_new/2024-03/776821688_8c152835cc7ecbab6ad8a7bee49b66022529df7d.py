class Solution:
    def maxProfit(self, prices: List[int]) -> int:
        buy=prices[0]
        ans=0
        for i in prices:
            if i>buy:
                ans=max(i-buy,ans)
            else:
                buy=i
                
        return ans
                
