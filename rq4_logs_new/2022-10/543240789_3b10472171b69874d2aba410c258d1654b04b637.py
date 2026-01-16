
'''
Framework for Solving DP Problems:

1. Define the objective function
-- Lets define the scope of the problem and work with that ->

The scope og the problem is to get the distinct results of the problem for n
where we can take k number of steps at a time.

2. Idenitfy base cases
The base cases are ->
f(0)=1
f(1)=1 #Actually this is also not needed as it can be computed from the 
previous examples or the base cases.

3. Write down a recurrence relation for the optimized objective function

The reccurence relation here is ->

f(n-1)+f(n-2)+.....+f(n-k)

4. What's the order of execution?
The problem still uses the bottom up approach.

5. Where to look for the answer?
The answer can be found at n or it can also be found at 
n%k in case of the array of k for space optimization.

'''


# Time Complexity -> O(nk)
# Space Complexity -> O(n)

def climbstairs(n,k):
    dp = [0]*(n+1)
    dp[0]=1
    dp[1]=1
    for i in range(2,n+1):
        for j in range(1,k+1):
            if i+j<0:
                continue
            dp[i] = dp[i] + dp[i-j]
    return dp[-1]

# Further optimizing the problem we can reduce the space complexity
# to O(k)

# Space Complexity -> O(k)

def climbstairso(n,k):
    dp = [0]*(k+1)
    dp[0] = 1
    for i in range(1,n+1):
        for j in range(k):
            if i+j < 0:
                continue
            dp[i%k] = dp[i]+dp[(i+j)%k]
    return dp[n%k]