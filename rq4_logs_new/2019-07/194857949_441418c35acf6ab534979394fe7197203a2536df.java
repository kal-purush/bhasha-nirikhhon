public class Q12 {
    public static int getNumOfDivisors(int N) {
        int num = 0;
        for (int i = 1; i <= N; i++) {
            if (N % i == 0) {
                num++;
            }
        }
        // System.out.println(num);
        return num;
    }

    public static void main(String[] args) {
        final int DIVISORS = 500;
        int triangular = 0;
        int i = 1;

        triangular += i;
        while (getNumOfDivisors(triangular) <= DIVISORS) {
            if (i % 1000 == 0)
                System.out.println("段数 : " + i + " 数 : " + triangular);
            i++;
            triangular += i;
        }

        System.out.println(triangular);
    }
}