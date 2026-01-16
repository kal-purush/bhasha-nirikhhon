//Checking whether a given number is an armstrong number or not

import java.util.Scanner;
public class ArmstrongNumber
{
    public static boolean armstrong(int n)
    {
        //temp stores the value of n to keep it reserved.
        int temp=n;
        //to store last digit of reduced number.
        int digit;
        int sum = 0; //to store the value of sum
        while(n>0)
        {
            digit = n%10;
            sum = sum + digit*digit*digit;
            n = n/10;
        }
        if(sum == temp)
            return true;
        else
            return false;
    }
    public static void main(String args[])
    {
        Scanner input = new Scanner(System.in);
        System.out.println("Enter the number");
        int num = input.nextInt();
        input.close();
        System.out.println("The number is Armstrong number: " + armstrong(num));
    }
}