package com.yearup.week2;
import java.util.Scanner;
public class FibonacciSeries {
	/*
	Fibonacci Series to print the Fibonacci number upto the user input value.
	Initialization: i=0 j=0 fib =1
    i=fib
    fib = fib+j
    j=i
     Result for each iteration.
    i=0, fib =1,j=0
    i=1, fib=1, j=1
    i=1, fib=2, j=1
    i=2, fib=3, j=2
    i=3, fib=5, j=3
     */
  public static void main(String[] args)
  {
		int initialValue =0; //declaration & variable initialization
		int tempVariable =0; 
		int fibonacciNumber = 1;
		int userInput = 0;
		Scanner sc = new Scanner(System.in);
	    System.out.println("Enter the largest number of your fibonacci series");
		userInput = sc.nextInt();
		while (initialValue < userInput)
		{
	System.out.println(fibonacciNumber);
	initialValue = fibonacciNumber;
	fibonacciNumber = fibonacciNumber + tempVariable;
	tempVariable = initialValue;
		}
  }
}
