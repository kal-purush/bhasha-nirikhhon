/**
 * 零钱兑换
 * 动态规划
 */

import java.util.ArrayList;

class Solution {
    public int coinChange(int[] coins, int amount) {
        int coinNum = coins.length;
        int[] result = new int[amount + 1];


        for (int i = 1; i < amount + 1; ++i) {
            int[] choices = new int[coinNum];

            for (int j = 0; j < coinNum; ++j) {

                if (i - coins[j] < 0) {
                    choices[j] = -1;
                }
                else if (i - coins[j] == 0) {
                    choices[j] = 1;
                }
                else {
                    if (result[i - coins[j]] == -1) {
                        choices[j] = -1;
                    }
                    else {
                        choices[j] = 1 + result[i - coins[j]];
                    }

                }
            }
            int min = min(choices);
            result[i] = min;
        }

        return result[amount];
    }

    public int min(int[] nums) {
        ArrayList<Integer> positives = new ArrayList<>();
        for (int i = 0; i < nums.length; ++i) {
            if (nums[i] > 0)
                positives.add(nums[i]);
        }

        if (positives.size() == 0)
            return -1;

        int min = positives.get(0);
        for (Integer p : positives) {
            if (min > p)
                min = p;
        }

        return min;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        int[] coins = {2};
        int amount = 3;
        System.out.println(s.coinChange(coins, amount));
    }

}