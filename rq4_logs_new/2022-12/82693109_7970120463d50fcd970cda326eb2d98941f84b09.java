package leetcode.p0931;

// n == matrix.length == matrix[i].length
// 1 <= n <= 100
// -100 <= matrix[i][j] <= 100
class Solution {

    private final int[] rowPaths = {1, 1, 1};
    private final int[] colPaths = {-1, 0, 1};
    private int minSumSoFar = 10001;

    public int minFallingPathSum(int[][] matrix) {
        final int n = matrix.length;
        // horizontal scanning
        for (int i = 0; i < n; i++) {
            verticalVisit(matrix, n, 0, i, 0, 0);
        }
        return minSumSoFar;
    }

    // vertical scanning
    // backtracking from current matrix[x][y]
    // go deeper, and make another choice after return to the current
    private void verticalVisit(int[][] matrix, int n, int x, int y, int sumSoFar, int currDepth) {
        if (x == n || y == n || y < 0) {
            if (currDepth == n) {
                // reached to the bottom
                minSumSoFar = Math.min(minSumSoFar, sumSoFar);
            }
            return;
        }
        for (int i = 0; i < 3; i++) {
            verticalVisit(matrix, n, x + rowPaths[i], y + colPaths[i], sumSoFar + matrix[x][y], currDepth + 1);
        }
    }
}