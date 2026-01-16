import edu.princeton.cs.algs4.StdIn;
import edu.princeton.cs.algs4.StdOut;
import edu.princeton.cs.algs4.In;

import java.util.Arrays;

class BinarySearch {
	public static int rank(int key, int[] a) {
		// Array must be sorted.
		int lo = 0;
		int hi = a.length - 1;
		int mid;

		while (lo <= hi) {
			// Key is in a[lo..hi] or not present.
			mid = lo + (hi - lo) / 2;
			if (key < a[mid]) {
				hi = mid - 1;
			} else if (key > a[mid]) {
				lo = mid + 1;
			} else {
				return mid;
			}
		}
		return -1;
	}

	public static void main(String[] args) {
		int[] whitelist = new In(args[0]).readAllInts();

		Arrays.sort(whitelist);

		int key;
		while (!StdIn.isEmpty()) {
			key = StdIn.readInt();
			if (rank(key, whitelist) < 0) {
				StdOut.println(key);
			}
		}
	}
}