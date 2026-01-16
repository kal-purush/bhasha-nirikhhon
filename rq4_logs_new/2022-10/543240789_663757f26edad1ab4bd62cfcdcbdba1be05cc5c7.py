'''
Problem:
Climbing Stairs
You are climbing a stair case. It takes n steps to reach to the top.
Each time you can either climb 1 or 2 steps.

In how many distinct ways can you climb to the top?

Framework for Solving DP Problems:

1. Define the objective function
-- Lets define the scope of the problem and work with that ->
F(i) is the number of distinct ways to reach the ith stair.

2. Idenitfy base cases
The bases cases are ->
F(0) is 1
F(1) is 1
F(2) is 2

3. Write down a recurrence relation for the optimized objective function
F(n) = F(n-1) + F(n-2) 

4. What's the order of execution?
 This problem uses a bottom up approach.

5. Where to look for the answer?
The answer is in f(n).
'''

def climbstairs(n):
    dp = [None] * (n+1)
    dp[0] = 1
    dp[1] = 1
    for i in range(2,n+1):
        dp[i] = dp[i-1] + dp[i-2]
    return dp[n]