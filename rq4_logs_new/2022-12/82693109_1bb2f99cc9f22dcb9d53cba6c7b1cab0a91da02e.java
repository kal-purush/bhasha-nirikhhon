package leetcode.p0931.backtracking.functional;

// n == matrix.length == matrix[i].length
// 1 <= n <= 100
// -100 <= matrix[i][j] <= 100
class Solution {
    public int minFallingPathSum(int[][] matrix) {
        final int n = matrix.length;
        int minSumSoFar = 10001;
        // horizontal scanning
        for (int i = 0; i < n; i++) {
            minSumSoFar = Math.min(minSumSoFar, verticalVisit(matrix, 0, i));
        }
        return minSumSoFar;
    }

    // vertical scanning
    // backtracking from current matrix[x][y]
    // go deeper, and make another choice after return to the current
    private int verticalVisit(int[][] matrix, int x, int y) {
        // base case
        if (x == matrix[0].length - 1) {
            // the top row is a starting point of vertical accumulation
            return matrix[x][y];
        }
        if (y == 0) {
            // cannot go to the left
            int bottom = verticalVisit(matrix, x + 1, y);
            int bottomRight = verticalVisit(matrix, x + 1, y + 1);
            return Math.min(bottom, bottomRight) + matrix[x][y];
        }
        if (y == matrix.length - 1) {
            // cannot go to the right
            int bottom = verticalVisit(matrix, x + 1, y);
            int bottomLeft = verticalVisit(matrix, x + 1, y - 1);
            return Math.min(bottom, bottomLeft) + matrix[x][y];
        }
        // can go anywhere
        int bottom = verticalVisit(matrix, x + 1, y);
        int bottomRight = verticalVisit(matrix, x + 1, y + 1);
        int bottomLeft = verticalVisit(matrix, x + 1, y - 1);
        return Math.min(bottom, Math.min(bottomLeft, bottomRight)) + matrix[x][y];
    }
}