class MinCostClimbStairs {
    public int minCostClimbingStairs(int[] cost) {
        if(cost.length ==0 || cost.length == 1)
            return 0;
        int[] dp = new int[cost.length+1];
        dp[0] = cost[0];
        dp[1] = cost[1];
        for(int i = 2; i<dp.length;i++){
            dp[i] = Math.min(dp[i-1],dp[i-2])+ ((i==dp.length-1)?0:cost[i]);
        }
        return dp[dp.length-1];
    }
}

//time complexity O(n) where n is the number of steps.
//Question. each step has some cost. at a time we can go 1 or 2 steps. starting index 0 or index 1. determine lowest cost to climb stairs