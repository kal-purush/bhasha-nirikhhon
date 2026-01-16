public class RegexMatching {
    class Solution {
        public boolean isMatch(String text, String pattern) {
            int m = text.length();
            int n = pattern.length();
            boolean[][] dp = new boolean[m + 1][n + 1];
            return isMatchRecursive(text, pattern, 0, 0, dp);
        }

        private boolean isMatchRecursive(String text, String pattern, int i, int j, boolean[][] dp) {
            int m = text.length();
            int n = pattern.length();

            if (j == n) {
                return i == m;
            }

            if (dp[i][j]) {
                return dp[i][j];
            }

            boolean match = i < m && (text.charAt(i) == pattern.charAt(j) || pattern.charAt(j) == '.');

            if (j + 1 < n && pattern.charAt(j + 1) == '*') {
                dp[i][j] = isMatchRecursive(text, pattern, i, j + 2, dp)
                        || (match && isMatchRecursive(text, pattern, i + 1, j, dp));
            } else {
                dp[i][j] = match && isMatchRecursive(text, pattern, i + 1, j + 1, dp);
            }

            return dp[i][j];
        }
    }

}