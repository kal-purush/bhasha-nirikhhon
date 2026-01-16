public class Q71 {

    public static boolean checkIrreducibleFraction(int numerator, int denominator) {
        boolean irreducible = true;
        for (int i = 2; i <= numerator; i++) {
            if (numerator % i == 0 && denominator % i == 0) {
                irreducible = false;
                break;
            }
        }
        return irreducible;
    }

    public static void main(String[] args) {
        final int LIMIT = 1000000;

        int miniDenominator = 5;
        int miniNumerator = 2;

        for (int i = LIMIT; i >= 9; i--) {
            int numerator = (int) ((3.0 * (double) i - 1.0) / 7.0);

            if ((long) miniNumerator * (long) i < (long) numerator * (long) miniDenominator) {
                if (checkIrreducibleFraction(numerator, i)) {
                    miniDenominator = i;
                    miniNumerator = numerator;
                }
            }
        }
        System.out.println(miniNumerator);
        System.out.println(miniDenominator);
    }
}