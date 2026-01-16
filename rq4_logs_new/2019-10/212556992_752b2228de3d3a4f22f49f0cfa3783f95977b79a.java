class Solution {
    public int reverse(int x) {
        long result = 0;

        long temp = 0;
        while (x != 0) {
            temp = temp * 10 + x % 10;
            if (temp > Integer.MAX_VALUE || temp < Integer.MIN_VALUE) {
                return 0;
            }
            result = temp;
            x = x / 10;
        }

        return (int) result;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        System.out.println(s.reverse(2147483647));
    }
}