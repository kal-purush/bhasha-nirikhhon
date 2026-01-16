package lessons.l5;

public class MinAvgTwoSlice {
	public int solution(int[] A) {
		int minP = 0;
		float minAvg = Integer.MAX_VALUE;
		int N = A.length;
		for (int P = 0; P < N; P++) {
			int p1Val = A[P];
			int slice = 1;
			for (int Q = P + 1; Q < N && P < Q; Q++) {
				slice++;
				if (slice > 3) {
					break;
				}
				int p2Val = A[Q];
				p1Val += p2Val;
				float tmpAvg = (float) p1Val / slice;
				if (tmpAvg < minAvg) {
					minAvg = tmpAvg;
					minP = P;
				}
			}
		}
		return minP;
	}
}