/**
By Now:
- Generate file of 1000 lines, each line has 2 random number
**/

import edu.princeton.cs.algs4.Out;
import edu.princeton.cs.algs4.StdRandom;
class DataGenerator {
	public static void twoRandoms(String filename) {
		Out out = new Out(filename);
		final int L = 100000;		// Max line numbers
		final int U = 99999999;	// Random union [0, U]

		for (int i = 0; i < L; ++i) {
			out.println(StdRandom.uniform(U) + " " + StdRandom.uniform(U));
		}
		out.close();
	}

	public static void main(String[] args) {
		twoRandoms(args[0]);
	}
}