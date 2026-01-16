import java.util.*;

public class fib_last_digit {
    private static int getFibonacciLastDigitNaive(int n) {
	    int[] fib=new int[n+1];
        if (n <=1)
            return 1;

         fib[0] = 0;
         fib[1]  = 1;

        for (int i = 2; i <=n; i++) {
		
		fib[i]=(fib[i-1]+fib[i-2])%10;
		
		
        }

        return fib[n];
    }
    
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        int c = getFibonacciLastDigitNaive(n);
        System.out.println(c);
    }
}
