//simple dp algorithm. coin change: find minimum number of coins for a given amount.
// Read article and used dp bottom-up approach. calculate the less amount with min coins and sum up.
// F(n) = Math.min(F(n-c1),F(n-c2),F(n-c3)....F(n-ci))+1;
//TC = O(sn) where s is amount and n is coins.
//SC = O(s)

class CoinChange {
    public int coinChange(int[] coins, int amount) {
        //dynamic programming bottom -up approach.
        int[] dp = new int[amount+1];
        int max = amount+1;
        Arrays.fill(dp,max);
        dp[0] =0;
        for(int j =1; j<=amount; j++){
            for(int i = 0; i<coins.length;i++){
                if(coins[i]<=j){
                    dp[j] = Math.min(dp[j],dp[j-coins[i]]+1);
                }
            }
        }
        return (dp[amount]>amount)?-1:dp[amount];
    }
}