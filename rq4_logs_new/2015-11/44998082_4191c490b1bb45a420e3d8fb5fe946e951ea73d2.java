
/*
 * This class shows the Java implementation of Binary search on a
 * sorted int array.
 * Created by Aakash Sethi
 * on 2015-11-16
 */

import java.util.Arrays;

public class BinarySearch {

	protected static final int FAILURE = -1;

	public static void main(String[] args) {
		int[] array = new int[100];

		for(int i=0; i < array.length; i++) {
			array[i] = (int) (Math.random()*100 + 1);
		}

		Arrays.sort(array);

		int key = 32;    // This is a random number I decided to search
		int result = bSearch(array, 0, array.length-1, key);    // This gives the result of the search

		if(result < 0)
			System.out.println("Did not find the element!");
		else
			System.out.println("Found the element at " + result + " index of the array.");
	}

	/*
	 * This is a recursive method to search for an element findMe using binary search algorithm.
	 */
	public static int bSearch(int[] a, int left, int right, int findMe) {

		if(left > right)
			return FAILURE;

		int mid = (left + right) / 2;

		if(a[mid] == findMe)
			return mid;
		else if(a[mid] > findMe)
			return bSearch(a, left, mid-1, findMe);
		else if(a[mid] < findMe)
			return bSearch(a, mid+1, right, findMe);
		
		return -1;
	}
}