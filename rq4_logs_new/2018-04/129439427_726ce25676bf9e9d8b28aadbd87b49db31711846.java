package Zd1;

import java.util.Scanner;

public class SumToN {

    public static void main(String[] args)
    {
        System.out.println("Enter number:");
        Scanner scanner = new Scanner(System.in);
        int result = 0,n= scanner.nextInt();
        for(int i = 0;i<= n;i++)
            result +=i;
        System.out.printf("Sum of number (to number %d) is %d",n,result);
    }
}