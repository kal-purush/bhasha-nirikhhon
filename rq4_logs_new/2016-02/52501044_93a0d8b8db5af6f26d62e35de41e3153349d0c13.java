package myUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class ArrayOps {

	public static int whichMin(double[] arr) {
		
		int min_idx = -1;
		double min = Double.POSITIVE_INFINITY;
		for (int i = 0; i < arr.length; i++) {
			if (arr[i] < min) {
				min_idx = i;	min = arr[i];
			}
		}
		return min_idx;
	}

	public static int whichMax(double[] arr) {
		int max_idx = -1;
		double max = Double.NEGATIVE_INFINITY;
		for (int i = 0; i < arr.length; i++) {
			if (arr[i] > max) {
				max_idx = i;	max = arr[i];
			}
		}
		return max_idx;
	}
	// pre: start <= end
	// create consecutive integers from start (inclusive) up to end (exclusive)
	public static int[] intsInRange(int start, int end) {
		int[] arr = new int[end - start];
		for (int i = start; i < end; i++) {
			arr[i - start] = i;
		}
		return arr;
	}

	/**
	 * Check if two arrays are actually the same set, only different in order 
	 * i.e. they are two different permutations of the same set
	 *  (e.g. {3,2,1} is the same as {1,2,3}) 
	 *  Idea: their sorted versions should be the same
	 * @param a1
	 * @param a2
	 * @return
	 */
	public static boolean isEqual(Double[] a1, Double[] a2) {
		// TODO Auto-generated method stub
		Set<Double> set1 = new HashSet<>(Arrays.asList(a1));
		Set<Double> set2 = new HashSet<>(Arrays.asList(a2));
		
		return SetOps.isEqual(set1, set2);
	}

	public static boolean isEqual(int[] a1, int[] a2) {
		Arrays.sort(a1);	Arrays.sort(a2);
		return Arrays.equals(a1, a2);
	}

	public static boolean allLessThan(double[] arr, double v) {
		int i = -1;
		boolean all_smaller = true;
		while (all_smaller && i < arr.length) {
			i++;
			all_smaller = (arr[i] < v);
		}
		return all_smaller;
	}
}