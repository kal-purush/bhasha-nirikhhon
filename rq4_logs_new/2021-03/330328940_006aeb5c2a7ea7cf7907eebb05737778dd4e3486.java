package DP;

public class LIS_set {
    //300
    public int lengthOfLIS(int[] nums) {
        int ans = 0;
        int n = nums.length;
        int dp[] = new int[n];
        for(int i = 0; i < n; i++){
            int temp = lengthOfLIS_rec(nums, i,dp);
            ans = Math.max(temp, ans);
        }
        return ans;
    }

    public int lengthOfLIS_rec(int[] nums, int idx, int[] dp) {
        if(dp[idx] != 0) return dp[idx];

        int ans = 0;
        for(int i = idx - 1; i >= 0; i--){
            if(nums[i] < nums[idx]){
                int rec = lengthOfLIS_rec(nums, i, dp);

                ans = Math.max(rec, ans);
            }
        }
        return dp[idx] = ans + 1;
    }

    public int lengthOfLIS_DP(int[] arr) {
        int n = arr.length;
        int[] dp = new int[n];
        int len = 0;
        for (int i = 0; i < n; i++) {
            dp[i] = 1;
            for (int j = i - 1; j >= 0; j--) {
                if (arr[j] < arr[i]) {
                    dp[i] = Math.max(dp[i], dp[j] + 1);
                }
            }

            len = Math.max(len, dp[i]);
        }

        return len;
    }
    // minimum Number of deletion to be performed to make array sorted (array contain -1e7 <= ele <= 1e7)
    public static int minDeletion(int[] arr){
        int n = arr.length;
        int[] dp = new int[n];
        int len = 0;
        for (int i = 0; i < n; i++) {
            dp[i] = 1;
            for (int j = i - 1; j >= 0; j--) {
                if (arr[j] <= arr[i]) {
                    dp[i] = Math.max(dp[i], dp[j] + 1);
                }
            }

            len = Math.max(len, dp[i]);
        }

        return n - len;
    }

//https://practice.geeksforgeeks.org/problems/maximum-sum-increasing-subsequence4749/1

    public int maxSumIS(int arr[], int n)  
    {  
        //code here.
        int[] dp = new int[n];
        int ans = 0;
        for(int i = 0; i < n; i++){
            int callAns = maxSumsIS(arr, i, dp);

            ans = Math.max(callAns, ans);
        }
        return ans;
    }

    public int maxSumsIS(int arr[], int idx, int[] dp){
        
        if(dp[idx] != 0) return dp[idx];

        int maxSum = 0;
        for(int i = idx - 1; i >= 0; i++){
            if(arr[i] < arr[idx]){ 
                int ans = maxSumsIS(arr, i, dp);
    
                maxSum = Math.max(maxSum, ans);
            }
        }

        return dp[idx] = maxSum + arr[idx];
    }

    public int maxSumsIS_DP(int arr[], int n){
        int[] dp = new int[n];

        int ans = 0;
        
        for(int i = 0; i < n; i++){
            dp[i] = arr[i];

            for(int j = i - 1; j >= 0; j--){
                if(arr[j] < arr[i]){
                    dp[i] = Math.max(dp[i], dp[j] + arr[j]);
                }
            }
            ans = Math.max(dp[i], ans);
        }
        return ans;
    }

}