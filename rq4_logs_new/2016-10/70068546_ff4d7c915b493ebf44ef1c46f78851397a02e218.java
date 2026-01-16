package arrays.maxAbsoluteDifference;

import java.util.ArrayList;

/**
 * You are given an array of N integers, A1, A2 ,…, AN. Return maximum value of f(i, j) for all 1 ≤ i, j ≤ N.
 * f(i, j) is defined as |A[i] - A[j]| + |i - j|, where |x| denotes absolute value of x.
 * f(i, j) = |A[i] - A[j]| + |i - j| can be written in 4 ways (Since we are looking at max value,
 * we don’t even care if the value becomes negative as long as we are also covering the max value in some way).
 * 			(A[i] + i) - (A[j] + j)
 * 			-(A[i] - i) + (A[j] - j)
 * 			(A[i] - i) - (A[j] - j)
 * 			(-A[i] - i) + (A[j] + j) = -(A[i] + i) + (A[j] + j)
 * 	 Note that case 1 and 4 are equivalent and so are case 2 and 3.
 * We can construct two arrays with values: A[i] + i and A[i] - i.
 * Then, for above 2 cases, we find the maximum value possible.
 * For that, we just have to store minimum and maximum values of expressions A[i] + i and A[i] - i for all i.
 *
 *
 * Fix it for A : [ -70, -64, -6, -56, 64, 61, -57, 16, 48, -98 ]
 *
 * C++ code
 *int Solution::maxArr(vector<int> &A) {
 int ans = 0, n = A.size();

 int max1 = INT_MIN, max2 = INT_MIN;
 int min1 = INT_MAX, min2 = INT_MAX;

 for(int i = 0; i < n; i++){
 max1 = max(max1, A[i] + i);
 max2 = max(max2, A[i] - i);
 min1 = min(min1, A[i] + i);
 min2 = min(min2, A[i] - i);
 }
 ans = max(ans, max2 - min2);
 ans = max(ans, max1 - min1);
 return ans;
 }
 *
 */


public class MaxAbsoluteDifference {
	public int maxArr(ArrayList<Integer> A) {
		int ans = 0, n = A.size();

		int max1 = Integer.MIN_VALUE, max2 = Integer.MIN_VALUE;
		int min1 =  Integer.MAX_VALUE, min2 = Integer.MAX_VALUE ;

		for (int i = 0; i < n; i++) {
			max1 = max(max1, A.get(i) + i);
			max2 = max(max2, A.get(i) - i);
			min1 = min(min1, A.get(i) + i);
			min2 = min(min2, A.get(i) - i);
		}
		ans = max(ans, max2 - min2);
		ans = max(ans, max1 - min1);
		return ans;
	}

	public int max(int i, int j) {
		return i > j ? Math.abs(i) : Math.abs(j);
	}

	public int min(int i, int j) {
		return i > j ? j : i;
	}



	public static void main(String[] args) {
		ArrayList<Integer> X = new ArrayList<Integer>(3);

		X.add(1);X.add(3);X.add(-1);

		MaxAbsoluteDifference minStepsInInfiniteGrid = new MaxAbsoluteDifference();

		System.out.println(minStepsInInfiniteGrid.maxArr(X));
	}

}