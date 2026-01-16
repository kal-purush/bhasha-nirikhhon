package recursion;

public class NumberOfZero {
    public static void main(String[] args) {
        int num = 300040;
        System.out.println(getZeroCount(num));
    }

    public static int getZeroCount(int num) {
        return helper(num,0);
    }

    public static int helper(int n, int c) {
        if (n == 0) {
            return c;
        }
        int rem = n % 10;
        if (rem == 0) {
            return helper(n / 10, c + 1);
        }
        return helper(n / 10, c);
    }
}