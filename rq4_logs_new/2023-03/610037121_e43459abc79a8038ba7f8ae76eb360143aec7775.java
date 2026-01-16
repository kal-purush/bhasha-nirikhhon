package lc048;

public class Solution {
    public void rotate(int[][] matrix) {
        int n = matrix.length;
        int[][] ans = new int[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                ans[j][n-i-1] = matrix[i][j];
            }
        }
        for (int i = 0; i < n; i++) {
            System.arraycopy(ans[i], 0, matrix[i], 0, n);
        }
    }
}