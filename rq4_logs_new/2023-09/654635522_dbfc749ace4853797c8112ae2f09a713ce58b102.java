// print the x^n in the O(log n) complexity.

public class prob10 {
    public static void main(String[] args) {
        System.out.println("hello world");
        System.out.println(optimizedPower(2, 5));
        System.out.println(optimizedPower(2, 4));

    }

    public static int optimizedPower(int x, int n) {
       
        
        if (n == 0)
            return 1;

        // the logic is to multiply n/2 * n/2 to form n .

        //int halfPow = optimizedPower(x, n / 2) * optimizedPower(x, n / 2);
        
        int halfPow = optimizedPower(x, n / 2);

        int halfPowsq = halfPow * halfPow;
        if (n % 2 == 0) {
            return halfPowsq;
        }
        if (n % 2 != 0) {
            return x * halfPowsq ;
        }
        return -1;
    }
}