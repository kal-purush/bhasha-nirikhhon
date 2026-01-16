
import java.util.Scanner;

public class FactorialRecursive {

    public static void main(String[] args) {
                System.out.print("Enter the number:");
		Scanner s= new Scanner (System.in);
		int n=s.nextInt();
                System.out.println("Factorial of "+n+ "=" + compute(n));
    }

    public static int compute(int num) {
        // this is the method that student codes up
        int fact=1;
                if (num >= 1)
                    fact=num*compute(num-1);
                else if (num < 0){
                    System.out.println("Factorial of negative numbers are undefined");
                    System.exit(0);
                }
                return fact;
    }
}