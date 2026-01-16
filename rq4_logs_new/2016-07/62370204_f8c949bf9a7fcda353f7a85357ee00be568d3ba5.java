package com.spk.coll;

import java.util.Arrays;

public class MultiDimArray {
	
	public static void main(String[] args) {
		int[][] test = new int[][]{
	            {1, 1, 2, 2, 3, 4, 5},
	            {1, 1, 1, 1, 1, 1, 1},
	            {1, 2, 3, 4, 5, 6, 7},
	            {1, 2, 1, 1, 1, 1, 1},};
		
		for (int[] input : test) {
            //System.out.println("MultDim to SingleDim Array: " + Arrays.toString(input));
            System.out.println("After removing duplicates   : " + Arrays.toString(removeDuplicates(input)));
        }
	}
	
	public static int[] removeDuplicates(int[] numbersWithDuplicates){
		
		// Sorting array to bring duplicates together
		Arrays.sort(numbersWithDuplicates);
		
		int[] result = new int[numbersWithDuplicates.length];
		int previous = numbersWithDuplicates[0];
		result[0] = previous;
		
		for (int i = 1; i < numbersWithDuplicates.length; i++){
			int ch = numbersWithDuplicates[i];
			if (previous != ch){
				result[i] = ch;				
			}
			previous = ch;
		}
		return result;		
		
	}
}