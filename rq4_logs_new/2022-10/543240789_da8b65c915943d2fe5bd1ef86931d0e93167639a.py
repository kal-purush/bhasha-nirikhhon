'''
Problem Statement -

We need to get to the nth step and we can skip k stairs find all the possible outcomes
where we cannot step on a few of the steps.

Framework for Solving DP Problems:

1. Define the objective function
-- Lets define the scope of the problem and work with that ->
Calc all the distinct ways to reach n where we can skip k steps and 
cannot step on a few of the steps.

2. Idenitfy base cases
The base cases are ->
dp[0]=1
dp[1]=1

3. Write down a recurrence relation for the optimized objective function
Reccurence Relation -> f(n-1)+f(n-2)+...+f(n-k).

4. What's the order of execution?
Bottom Up Approach.

5. Where to look for the answer?
The answer should lie in f(n).

'''

# Time Complexity -> O(nk)
# Space Complexity -> O(k)

def staircase(n,k,l=[]):
    dp = [0]*(k+1)
    dp[0] = 1
    for i in range(1,n+1):
        for j in range(1,k):
            if i-j < 0:
                continue
            if i in l:
                dp[i%k] = 0
            else:
                dp[i%k] = dp[i%k] + dp[(i-j)%k]
    return dp[n%k]
