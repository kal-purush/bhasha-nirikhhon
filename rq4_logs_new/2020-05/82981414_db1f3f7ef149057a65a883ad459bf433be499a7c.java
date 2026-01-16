package com.leetcode.dynamicprogramming.medium;

/**
 * Alex and Lee play a game with piles of stones.  There are an even number of piles arranged in a row, and each pile has a positive integer number of stones piles[i].
 * <p>
 * The objective of the game is to end with the most stones.  The total number of stones is odd, so there are no ties.
 * <p>
 * Alex and Lee take turns, with Alex starting first.  Each turn, a player takes the entire pile of stones from either the beginning or the end of the row.  This continues until there are no more piles left, at which point the person with the most stones wins.
 * <p>
 * Assuming Alex and Lee play optimally, return True if and only if Alex wins the game.
 * <p>
 * <p>
 * <p>
 * Example 1:
 * <p>
 * Input: [5,3,4,5]
 * Output: true
 * Explanation:
 * Alex starts first, and can only take the first 5 or the last 5.
 * Say he takes the first 5, so that the row becomes [3, 4, 5].
 * If Lee takes 3, then the board is [4, 5], and Alex takes 5 to win with 10 points.
 * If Lee takes the last 5, then the board is [3, 4], and Alex takes 4 to win with 9 points.
 * This demonstrated that taking the first 5 was a winning move for Alex, so we return true.
 * <p>
 * <p>
 * Note:
 * <p>
 * 2 <= piles.length <= 500
 * piles.length is even.
 * 1 <= piles[i] <= 500
 * sum(piles) is odd.
 */
public class StoneGame {

    public static boolean stoneGame(int[] piles) {
        int n = piles.length;
        int[][] dp = new int[n][n];

        /**
         * ����Ŀǰ���������ܹ���������������͵Ľⷨ
         * https://www.cnblogs.com/qq874455953/p/9901067.html
         * ��DP����⣬��ʵ���˵�ʱ����ȫ�����Ȿ������С�ӽṹ���Լ�״̬ת�Ʒ��̵�ȷ��
         */
        for (int i = 0; i < n; i++) {
            dp[i][i] = piles[i];
        }
        /**
         * ��������forѭ�������Ǵ���С�ӽṹ��ʼ����2��С�ѵ�dp���Ž⣬�㵽n��С��
         * ��η��������Ⱥ���ȷ��������һ��������ѡ��ѡ��֮�����n-1��piles
         * dp[i,j]����Ϊpiles[i]...piles[j]��������ܶ��ü���stone���������ǳ���Ҫ
         * Ȼ��ʼ״̬ת�Ʒ��̣����ѡ��A����piles[i]����ѡ��B��A����ܶ���dp[i+1][j]
         * �̶�A����piles[i]ʱ������ܱ�B���õľ���dp[i][j]=piles[i]-dp[i+1][j]
         * ��ʱһ����״̬ת�Ʒ��̾ͳ����ˣ���һ���־��ǵ�A����piles[j]����Ӧ��dp[i][j]=piles[j]-dp[i][j-1]
         * ����ȡ���ֵ����Ϊ����dp[i][j]
         */
        for (int dis = 1; dis < n; dis++) {
            for (int i = 0; i < n - dis; i++) {
                dp[i][i + dis] = Math.max(piles[i] - dp[i + 1][i + dis], piles[i + dis] - dp[i][i + dis - 1]);
            }
        }
        return dp[0][n - 1] > 0;
    }

    public static void main(String[] args) {
        System.out.println(stoneGame(new int[]{5, 3, 4, 5}));
    }
}