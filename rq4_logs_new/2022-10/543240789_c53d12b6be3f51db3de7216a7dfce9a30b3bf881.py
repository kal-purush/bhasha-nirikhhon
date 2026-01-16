'''
1. Define the objective function
-- Lets define the scope of the problem and work with that ->
The scope of the problem is to get the minimum cost of climbing the stairs when
we cann skip max of 2 steps. 

2. Idenitfy base cases
The base cases are ->
dp[0]=0
dp[1]=l[1] Given that l is the list of costs.

3. Write down a recurrence relation for the optimized objective function
dp[i]=min(dp[i-1],dp[i-2])+l[i]

4. What's the order of execution?
The order of execution is Bottom Up 

5. Where to look for the answer?
We need to check dp[-1] or dp[n]
'''


def stairs(n,l):
    l = [0]+l
    dp = [0]*(n+1)
    dp[0] = l[0]
    dp[1] = l[1]
    for i in range(2,n+1):
        dp[i]= min(dp[i - 1],dp[i - 2]) + l[i]
    return dp[-1]

# Optimized Space Complexity - O(1).

def stairso(n,l):
    l = [0]+l
    temp1 = l[0]
    temp2 = l[1]
    temp3 = 0
    for i in range(2,n+1):
        temp3= min(temp1,temp2) + l[i]
        temp1 = temp2
        temp2 = temp3
    return temp2