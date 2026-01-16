/**
 * 回文数
 */
class Solution {
    public boolean isPalindrome(int x) {
        if (x < 0 || (x > 0 && x % 10 == 0)) {
            return false;
        }

        return x == reverse(x);
    }

    public int reverse(int x) {
        int reverse = 0;
        while (x != 0) {
            reverse = reverse * 10 + x % 10;
            x /= 10;
        }

        return reverse;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        System.out.println(s.isPalindrome(0));
    }
}