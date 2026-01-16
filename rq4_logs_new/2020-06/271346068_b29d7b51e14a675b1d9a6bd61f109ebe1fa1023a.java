package com.sow.arrays;

import java.io.*;
import java.math.*;
import java.security.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public class CakeCandles {

	// Complete the birthdayCakeCandles function below.
	static void birthdayCakeCandles(int[] ar) {
		
		int counter = 0;
		Arrays.sort(ar);
		int temp = ar[ar.length-1];
		
		for (int i = 0; i < ar.length; i++) {
			if (temp == ar[i]) {
				counter++;
			}
		}
		// return counter;
		System.out.println(counter);
	}

	private static final Scanner scanner = new Scanner(System.in);

	public static void main(String[] args) throws IOException {
		int[] arr = new int[10];

		String[] arrItems = scanner.nextLine().split(" ");
		scanner.skip("(\r\n|[\n\r\u2028\u2029\u0085])?");

		for (int i = 0; i < 10; i++) {
			int arrItem = Integer.parseInt(arrItems[i]);
			arr[i] = arrItem;
		}

		birthdayCakeCandles(arr);

	}
}