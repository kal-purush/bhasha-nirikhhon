//https://leetcode.com/problems/set-matrix-zeroes/
package algorithms.medium;

public class SetMatrixZeroes {
    public static void main(String[] args) {
        SetMatrixZeroes matrix = new SetMatrixZeroes();
        matrix.setZeroes(new int[][]{{1, 1, 1}, {1, 0, 1}, {1, 1, 1}});
        matrix.setZeroes(new int[][]{{0, 1, 2, 0}, {3, 4, 5, 2}, {1, 3, 1, 5}});
    }

    public void setZeroes(int[][] matrix) {
        boolean firstRow = false;
        boolean firstCol = false;
        int rows = matrix.length;
        int cols = matrix[0].length;

        for (int col = 0; col < cols; col++)
            if (matrix[0][col] == 0) firstRow = true;

        for (int row = 0; row < rows; row++)
            if (matrix[row][0] == 0) firstCol = true;

        for (int row = 1; row < rows; row++) {
            for (int col = 1; col < cols; col++) {
                if (matrix[row][col] == 0) {
                    matrix[0][col] = 0;
                    matrix[row][0] = 0;
                }
            }
        }

        for (int row = 1; row < rows; row++) {
            for (int col = 1; col < cols; col++) {
                if (matrix[0][col] == 0 || matrix[row][0] == 0) matrix[row][col] = 0;

            }
        }

        if (firstRow) for (int col = 0; col < cols; col++)
            matrix[0][col] = 0;

        if (firstCol) for (int row = 0; row < rows; row++)
            matrix[row][0] = 0;

    }
}