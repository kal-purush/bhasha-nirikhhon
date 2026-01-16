import edu.princeton.cs.algs4.StdOut;

public class Fibonacci {

	public static long F1(int N) {
		long prepre = 0, pre = 1, curr = 0;

		for (int i = 0; i < N; ++i) {
			curr = prepre + pre;
			prepre = pre;
			pre = curr;
		}
		return curr;
	}

	public static long F2(int N) {
		if (N == 0)
			return 0;

		long[] A = new long[N+1];
		A[0] = 0;
		A[1] = 1;
		for (int i = 2; i <= N; ++i) {
			A[i] = A[i-1] + A[i-2];
		}
		return A[N];
	}

	public static void main(String[] args) {
		long start, end, duration1, duration2;

		start = System.nanoTime();
		for (int N = 0; N < 90; ++N) {
			StdOut.println(N + " " + F1(N));
		}
		end = System.nanoTime();
		duration1 = end - start;

		StdOut.println("Duration: " + duration1);


		start = System.nanoTime();
		for (int N = 0; N < 90; ++N) {
			StdOut.println(N + " " + F2(N));
		}
		end = System.nanoTime();
		duration2 = end - start;

		StdOut.println("Duration: " + duration2);
		StdOut.println("Duration1 / Duration2 = " + 1.0 * duration1 / duration2);
	}
}