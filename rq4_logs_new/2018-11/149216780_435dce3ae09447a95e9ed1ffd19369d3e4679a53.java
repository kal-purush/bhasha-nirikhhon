package com.hussir.leetcode.part004;

import java.util.Arrays;

/**
 * Description:
 *
 *      Given a string containing just the characters '(' and ')', find the length of the longest valid (well-formed) parentheses substring.
 */
public class Question0032 {

    public int longestValidParentheses(String s) {

        if (s == null || s.length() <= 1) return 0;

        int[] dp = new int[s.length()];

        dp[0] = 0;
        dp[1] = 0;

        if (s.charAt(0) == '(' && s.charAt(1) == ')') {
            dp[1] = 2;
        }

        for (int i = 2; i < s.length(); i++) {

            if (s.charAt(i) == '(') {
                dp[i] = 0;
            } else {
                if (i - dp[i - 1] > 0 && s.charAt(i - dp[i - 1] - 1) == '('){
                    dp[i] = dp[i - 1] + 2 + (i - dp[i - 1] - 2 >= 0 ? dp[i - dp[i - 1] - 2] : 0);
                } else {
                    dp[i] = 0;
                }
            }
        }

        Arrays.sort(dp);

        return dp[dp.length - 1];
    }
}