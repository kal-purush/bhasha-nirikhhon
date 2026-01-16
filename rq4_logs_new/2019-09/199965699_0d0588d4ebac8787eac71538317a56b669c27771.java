
public class CyclicRotation {
	public static void main(String[] args) {
		int[] input = { 3, 8, 9, 7, 6 };
		int k = 3;
		int[] count = solution(input, k);

		for (int results : count) {
			System.out.print(results + ",");
		}
	}

	public static int[] solution(int[] A, int K) {
		if (A == null) {
			return null;
		}

		int[] output = new int[A.length];

		for (int i = 0; i < A.length; i++) {
			int rotated = (i + K) % A.length;
			System.out.println(rotated);
			output[rotated] = A[i];
		}
		return output;
	}
}