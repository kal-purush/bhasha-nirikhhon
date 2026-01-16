package com.assignment.nov25;

import java.util.Scanner;

/**
 * @author 799197
 *.       Create method to generate fibonacci series as per below inputs first 4 inputs: 0,1,1,2
 *		 and next number is going to be sum of last 4 numbers i.e. 4 (0+1+1+2).  Your program should print 
 *		n number of Fibonacci
		Use below skletoton:
		public static void printFibonancciSerices(int n) {
		//write your logic here
		}

 */
public class PrintFibonacci {
	public void printFibonacciSeries(int n) {
		int a = 0, b = 1, c = 1, d = 2;
		int e = 0;
		System.out.println(a + "\n" + b + "\n" + c + "\n" + d);
		for (int i = 4; i < n; i++) {
			e = a + b + c + d;
			a = b;
			b = c;										//Fibonacci Logic
			c = d;
			d = e;
			System.out.println(e);
		}

	}

	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		System.out.println("Enter value of n");
		int n = sc.nextInt();
		PrintFibonacci P = new PrintFibonacci();
		P.printFibonacciSeries(n);
		sc.close();
	}
}