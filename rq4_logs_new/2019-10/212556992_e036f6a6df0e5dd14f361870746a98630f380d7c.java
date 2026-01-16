class Solution {
    /**
     * 递归方法，但是超出时间限制
     * @param n
     * @return
     */
    public int __climbStairs(int n) {
        if (n < 0) {
            return -1;
        }

        if (n == 1) {
            return 1;
        }
        else if (n == 0) {
            return 1;
        }
        else {
            return __climbStairs(n - 1) + __climbStairs(n - 2);
        }

    }

    /**
     * 动态规划方法
     * dp[n] = dp[n - 1] + dp[n - 2]
     * @param n
     * @return
     */
    public int climbStairs(int n) {
        int size = n > 3 ? n : 3;
        int[] dp = new int[size + 1];
        dp[0] = 0;
        dp[1] = 1;
        dp[2] = 2;

        if (size > 2) {
            for (int i = 3; i < size + 1; ++i) {
                dp[i] = dp[i - 1] + dp[i - 2];
            }
        }

        return dp[n];
    }
}


class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        System.out.println(s.climbStairs(2));
        System.out.println(s.__climbStairs(2));

        for (int i = 1; i < 10; ++i) {
            if (s.climbStairs(i) != s.__climbStairs(i)) {
                System.err.println(i + "Not equal!");
            }
            else {
                System.out.println("Equal");
            }
        }

    }
}