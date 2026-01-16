package com.studyguide.problems;

public class OutOfBoundArrays {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//int nums[] = new int[1000000000]; //10^9 is execeding the value
		//Make size for Array, ArrayList, Vector => Interger.MAX_VALUE But for instance the size of array 10^9 will throw OutOfMemory issue because the OS cannot support is that not that JAVA cannot.
		int n = 1000000000;
		if(Integer.MAX_VALUE > n) System.out.println("n is small");
		else System.out.println("n is big or equal it max value");
	}

}