//-----------------------------------------------------
// Title: Programming Assignment 2 - Question 1 Insertion.java
// Author: Burak Güçlü
// Description: This class is used for applying Insertion sort.
//-----------------------------------------------------

public class Insertion {
	private static boolean less(Comparable v, Comparable w) {
		// --------------------------------------------------------
		// Summary: This method is being used for checking if the first
		// parameter is less than the second one.
		// Precondition: Comparable v is an array object that can be compared,
		// Comparable w is also an array object that can be compared.
		// Postcondition: It returns a number that represents the result
		// of comparison.
		// --------------------------------------------------------
		return v.compareTo(w) < 0;
	}

	private static boolean more(Comparable v, Comparable w) {
		// --------------------------------------------------------
		// Summary: This method is being used for checking if the first
		// parameter is more than the second one.
		// Precondition: Comparable v is an array object that can be compared,
		// Comparable w is also an array object that can be compared.
		// Postcondition: It returns a number that represents the result
		// of comparison.
		// --------------------------------------------------------
		return v.compareTo(w) > 0;
	}

	private static void exch(Comparable[] a, int i, int j) {
		// --------------------------------------------------------
		// Summary: This method is being used for exchanging the places
		// of two elements in the array.
		// Precondition: Comparable a is an array object that can be compared,
		// i and j are integer that represents the places of the elements.
		// Postcondition: It changes the items' places in the array a.
		// --------------------------------------------------------
		Comparable t = a[i];
		a[i] = a[j];
		a[j] = t;
	}

	public static void sort(Comparable[] a) { // Sort a[] into increasing order.
		// --------------------------------------------------------
		// Summary: This method is being used for sorting the items in the array
		// in increasing order.
		// Precondition: Comparable a is an array object that can be compared.
		// Postcondition: It sorts the array a in increasing order.
		// --------------------------------------------------------
		int N = a.length;
		for (int i = 1; i < N; i++) {
			for (int j = i; j > 0 && less(a[j], a[j - 1]); j--) // Check for comparions.
				exch(a, j, j - 1); // Change the place of elements.
		}
	}

	public static void decreasingSort(Comparable[] a) { // Sort a[] into decreasing order.
		// --------------------------------------------------------
		// Summary: This method is being used for sorting the items in the array
		// in decreasing order.
		// Precondition: Comparable a is an array object that can be compared.
		// Postcondition: It sorts the array a in decreasing order.
		// --------------------------------------------------------
		int N = a.length;
		for (int i = N - 1; i >= 0; i--) {
			for (int j = i; j < N - 1 && less(a[j], a[j + 1]); j++) // Check for comparions.
				exch(a, j, j + 1); // Change the place of elements.
		}
	}
}