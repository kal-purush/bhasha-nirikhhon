package problem906;

public final class SuperPalindromes {

    private static final int MAX_ITER = (int) Math.pow(3, 9);

    public int superpalindromesInRange(final String left, final String right) {
        final long leftDec = (long) Math.sqrt(Long.parseLong(left));
        final long rightDec = (long) Math.sqrt(Long.parseLong(right));

        int sNum = leftDec <= 3 && 3 <= rightDec ? 1 : 0;

        for (int i = 1; i < MAX_ITER; i++) {
            final long base10 = Long.parseLong(Long.toString(i, 3));

            if (base10 < leftDec)
                continue;
            else if (base10 > rightDec)
                return sNum;

            if (isLongPalindrome(base10) && isLongPalindrome(base10 * base10))
                ++sNum;
        }

        return sNum;
    }

    private boolean isLongPalindrome(final long num) {
        long num2 = 0;
        long p = num;
        do {
            num2 = 10 * num2 + p % 10;
        } while ((p /= 10) > 0);

        return num == num2;
    }

}