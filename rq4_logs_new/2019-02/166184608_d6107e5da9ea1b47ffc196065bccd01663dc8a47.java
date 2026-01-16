package DP;

/**
 * @author huzhu
 * @date created in 2019-02-18 11:06
 * @explaination 数组划分为两个相等的子集
 */
public class Code_416_PartitionEqualSubsetSum {

    public static boolean canPartition(int[] nums) {
        int sum = 0;
        for(int i : nums){
            sum += i;
        }

        if(sum % 2 != 0){
            return false;
        }

        //转换为0-1背包问题
        sum = sum / 2;
        int[] dp = new int[sum + 1];
        for(int i = 0;i < nums.length;i++){
            for(int j = sum;j >= nums[i];j--){
                dp[j] = Math.max(dp[j],dp[j - nums[i]] + nums[i]);
            }
        }
        return dp[sum] == sum;
    }

    public static void main(String[] args) {
        System.out.println(canPartition(new int[]{1, 5, 11, 5}));
        System.out.println(canPartition(new int[]{1, 2, 3, 5}));
    }
}