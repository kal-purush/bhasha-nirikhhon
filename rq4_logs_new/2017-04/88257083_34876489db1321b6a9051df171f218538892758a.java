package com.gmail.s12348.evgen;

public class GreateArray implements Runnable {
	private int[] testArray;
	private int begin;
	private int end;
	private int sum;

	public GreateArray() {
	}

	public GreateArray(int[] testArray, int begin, int end) {
		super();
		this.testArray = testArray;
		this.begin = begin;
		this.end = end;
	}

	public int getSum() {
		return sum;
	}

	public static int sumArray(int[] array) {
		int sum = 0;
		for (int i = 0; i < array.length; i++) {
			sum += array[i];
		}
		return sum;
	}

	@Override
	public void run() {
		for (int i = begin; i < end; i++) {
			sum += testArray[i];

		}
	}

}