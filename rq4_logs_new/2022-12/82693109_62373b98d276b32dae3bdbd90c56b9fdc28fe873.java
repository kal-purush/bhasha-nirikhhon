package leetcode.p0070.s2022.dp;

// Each time you can either climb 1 or 2 steps
// 1 <= n <= 45
class Solution {
    // n = 2,
    // 2 - 2 = 0 // 1 way
    // 2 - 1 = 1 // 1 way
    // dp[2] = 1 + 1 // 2 ways
    // n = 3,
    // 3 - 2 = 1 // 1 way
    // 3 - 1 = 2 // dp[2] = 2 // 2 ways
    // dp[3] = 1 + 2 // 3 ways
    // n = 4,
    // 4 - 2 = 2 // dp[2] = 2 // 2 ways
    // 4 - 1 = 3 // dp[3] = 3 // 3 ways
    // dp[4] = 2 + 3 // 5 ways
    public int climbStairs(int n) {
        int limit = 46;
        final int[] dp = new int[limit];
        dp[0] = 1; // base case
        dp[1] = 1; // base case
        for (int i = 2; i < limit; i++) {
            dp[i] = dp[i - 1] + dp[i - 2];
        }
        return dp[n];
    }
}