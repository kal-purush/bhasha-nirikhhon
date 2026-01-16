package com.sow.general;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class PatternProblem11 {

	public static void main(String args[]) throws Exception {
		// Enter data using BufferReader
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

		// Reading data using readLine
		Integer n = Integer.parseInt(reader.readLine().toString());
		if (n >= 1 && n <= 1000) {
			for (int i = 1; i <= n; i++) {
				for (int j = 0; j < i; j++) {
					System.out.print(i);
				}
			}
		}
	}
}