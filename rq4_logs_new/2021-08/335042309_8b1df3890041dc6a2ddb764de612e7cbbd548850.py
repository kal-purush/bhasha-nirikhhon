def max_profit(prices):

    n = len(prices)

    if n==1:
        return 0 

    max_prof = 0

    for i in range(n):
        curr_max = -1
        for j in range(i+1,n):
            if prices[j]>curr_max and prices[j]>prices[i]:
                curr_max = prices[j]
        
        if curr_max==-1:
            continue 
        else:
            if curr_max-prices[i]>max_prof:
                max_prof = curr_max-prices[i]
    
    return max_prof

prices = [7,1,5,3,6,4]
print(max_profit(prices))

prices = [7,6,4,3,1]
print(max_profit(prices))