package arrays.maxSumContiguousSubarray;

import java.util.ArrayList;
import java.util.List;

/**
 * Find the contiguous subarray within an array (containing at least one number) which has the largest sum.
 * For example:
 * Given the array [-2,1,-3,4,-1,2,1,-5,4],
 * the contiguous subarray [4,-1,2,1] has the largest sum = 6.
 * For this problem, return the maximum sum.
 *
 * O(n^3) solution is simple.
 * You look at every pair (i,j) and compute the sum of elements between i and j and then take the maximum of the sums.
 * Obviously, this is not the best solution.
 * Next improvement is O(n^2) when you notice that you don’t need to sum up all the elements between i and j.
 * You can just add A[j] to the sum you have already calculated in the previous loop from i to j-1.
 * However, we are looking for something better than N^2.
 * Let us say Ai, Ai+1 … Aj is our optimal solution.
 * Note that no prefix of the solution will ever have a negative sum.
 * Let us say Ai … Ak prefix had a negative sum.
 * Sum ( Ai Ai+1 … Aj ) = Sum (Ai … Ak) + Sum(Ak+1 … Aj)
 * Sum ( Ai Ai+1 … Aj) - Sum(Ak+1 … Aj) = Sum(Ai … Ak)
 * Now, since Sum(Ai … Ak) < 0,
 * Sum (Ai Ai+1 … Aj) - Sum (Ak+1 … Aj) < 0
 * which means Sum(Ak+1 … Aj ) > Sum (Ai Ai+1 … Aj)
 * This contradicts the fact that Ai, Ai+1 … Aj is our optimal solution.
 * Instead, Ak+1, Ak+2 … Aj will be our optimal solution.
 * Similarily, you can prove that for optimal solution, it is always good to include a prefix with positive sum.
 * Try to come up with a solution based on the previous 2 facts.
 *
 *TODO Watch the video
 *
 */
public class MaxSumContiguousSubarray {

	// DO NOT MODFIY THE LIST.
	public int maxSubArray(final List<Integer> a) {
		int max = a.get(0);
		int[] sum = new int[a.size()];
		sum[0] = a.get(0);

		for (int i = 1; i < a.size(); i++) {
			sum[i] = Math.max(a.get(i), sum[i - 1] + a.get(i));
			max = Math.max(max, sum[i]);
		}

		return max;
	}

	public static void main(String[] args) {
		ArrayList<Integer> X = new ArrayList<Integer>(3);

		X.add(1);X.add(3);X.add(-1);

		MaxSumContiguousSubarray maxSumContiguousSubarray = new MaxSumContiguousSubarray();

		System.out.println(maxSumContiguousSubarray.maxSubArray(X));
	}

}