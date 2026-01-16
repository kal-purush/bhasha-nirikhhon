package day17methodoverloadingdatetimeclasses;

import java.util.Scanner;

public class MethodCreation {
    public static void main(String[] args) {

        /*System.out.println("enter 2 numbers");
        Scanner input = new Scanner(System.in);
        int num1 = input.nextInt();
        int num2 = input.nextInt();

        adder(num1,num2);*/

        System.out.println("enter a word");
        Scanner input = new Scanner(System.in);
        String wrd = input.next();
        //firstLast(wrd);
        int result = sums(wrd);
        System.out.println(result);
    }


    public static void adder(int x, int y)
    {
    int sum = x+y;
        System.out.println(sum);
    }


    public static void firstLast (String s)
    {
        int lenght = s.length();
        char first = s.charAt(0);
        char last = s.charAt(lenght-1);
        System.out.println(first+" and "+last);

    }

    public static int sums(String s)
    {
        int sum=0;
        for (int i=0; i<s.length();i++)
        {
            sum=sum+s.charAt(i);
        }
        return sum;
    }



}