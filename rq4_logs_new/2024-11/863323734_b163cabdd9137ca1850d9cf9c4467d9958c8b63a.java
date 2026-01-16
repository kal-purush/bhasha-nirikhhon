class Solution {
    public long maxMatrixSum(int[][] matrix) {
        long sum = 0;
        int minAbs = Integer.MAX_VALUE;
        int negativeCount = 0;

        for (int[] row : matrix) {
            for (int num : row) {
                sum += Math.abs(num);  // Sum of absolute values
                if (num < 0) {
                    negativeCount++;  // Count negative numbers
                }
                minAbs = Math.min(minAbs, Math.abs(num));  // Track smallest absolute value
            }
        }

        // If negativeCount is odd, subtract twice the smallest absolute value
        if (negativeCount % 2 != 0) {
            sum -= 2 * minAbs;
        }

        return sum;
    }
}