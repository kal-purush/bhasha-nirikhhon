'''
You are given an array prices where prices[i] is the price of a given stock on the ith day.

You want to maximize your profit by choosing a single day to buy one stock and choosing
a different day in the future to sell that stock.

Return the maximum profit you can achieve from this transaction. If you cannot achieve any profit, return 0.

Example 1:

Input: prices = [7,1,5,3,6,4]
Output: 5
Explanation: Buy on day 2 (price = 1) and sell on day 5 (price = 6), profit = 6-1 = 5.
Note that buying on day 2 and selling on day 1 is not allowed because you must buy before you sell.
Example 2:

Input: prices = [7,6,4,3,1]
Output: 0
Explanation: In this case, no transactions are done and the max profit = 0.

'''
import time
def profit(lp):
    begin = time.time()
    #prof=[0]*(len(lp))
    # if lp[0]==max(lp):
    #     return 0
    # else:   
    prof = 0
    temp = 0 
    for i in range(len(lp)):
        #prof[i]=max(lp[i:])-lp[i]
        temp = max(lp[i:])-lp[i]
        if temp>prof:
            prof = temp
    end = time.time()
    print(f"Total runtime of the program is {end - begin}")
    return prof

#Optimized solution

def profito(lp):
    begin = time.time()
    
    if not lp:
        return 0
    maxProfit = 0
    minPurchase = lp[0]
    for i in range(1,len(lp)):
        maxProfit = max(maxProfit, lp[i]-minPurchase)
        minPurchase = min(minPurchase, lp[i])
    end = time.time()
    print(f"Total runtime of the program is {end - begin}")
    return maxProfit