import java.util.*;
import java.awt.List;
import java.io.*;
import java.math.*;

class Solution {

	public static void main(String args[]) {

		ArrayList<Integer> losses = new ArrayList<Integer>(100000); // 100,000 -
																	// max
																	// capacity
		ArrayList<Integer> values = new ArrayList<Integer>(100000); // array of
																	// given
																	// values

		Scanner in = new Scanner(System.in);
		int n = in.nextInt();
		for (int i = 0; i < n; i++) {
			int v = in.nextInt();
			values.add(i, v);
		}

		int i = 0; // helps keeping track of position in values
		int startPoint = values.get(i); // initial point where loss starts

		losses.add(0, 0); //in case there is no loss

		while (i < n - 1) {

			int m = values.get(i + 1) - startPoint; // difference between
													// current point and start

			if (values.get(i + 1) < startPoint && m <= losses.get(losses.size() - 1)) {
				losses.add(m);
				i++;
			}

			else if (values.get(i + 1) < startPoint && m > losses.get(losses.size() - 1)) {
				i++;
			}

			else if (values.get(i + 1) >= startPoint) {
				startPoint = values.get(i + 1);
				i++;
			}
		}

		Collections.sort(losses, new Comparator<Integer>() {

			@Override
			public int compare(Integer i2, Integer i1) {

				return i1.compareTo(i2);

			}
		});

		System.out.println(losses.get(losses.size() - 1));
	}
}