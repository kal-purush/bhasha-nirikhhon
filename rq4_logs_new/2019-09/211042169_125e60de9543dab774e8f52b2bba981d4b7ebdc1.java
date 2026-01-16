public class Leet70ClimbingStairs {
    static class Solution {
        public int climbStairs(int n) {
            if (n == 0 || n == 1) return 1;
            int curr = 2;
            int prev = 1;
            for (int i = 3; i <= n; i++) {
                int tmp = curr;
                curr = curr + prev;
                prev = tmp;
            }
            return curr;
        }
    }
}