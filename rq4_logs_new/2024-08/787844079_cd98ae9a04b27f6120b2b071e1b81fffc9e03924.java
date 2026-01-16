//https://leetcode.com/problems/reshape-the-matrix/description/
package algorithms.easy;

public class ReshapeTheMatrix {
    public static void main(String[] args) {
        ReshapeTheMatrix reshape = new ReshapeTheMatrix();
        System.out.println("Output:\t" + reshape.matrixReshapeBetterCoded(new int[][]{{1, 2}, {3, 4}}, 1, 4));
        System.out.println("Output:\t" + reshape.matrixReshapeBetterCoded(new int[][]{{1, 2}, {3, 4}}, 2, 4));
        System.out.println("Output:\t" + reshape.matrixReshape(new int[][]{{1, 2}, {3, 4}}, 1, 4));
        System.out.println("Output:\t" + reshape.matrixReshape(new int[][]{{1, 2}, {3, 4}}, 2, 4));
    }

    public int[][] matrixReshapeBetterCoded(int[][] mat, int r, int c) {
        int m = mat.length, n = mat[0].length;
        if (m * n != r * c) return mat;


        int[][] reshapedMatrix = new int[r][c];
        for (int i = 0; i < m * n; ++i) {
            int newRow = i / c;
            int newCol = i % c;
            int originalRow = i / n;
            int originalCol = i % n;
            reshapedMatrix[newRow][newCol] = mat[originalRow][originalCol];
        }
        return reshapedMatrix;
    }

    public int[][] matrixReshape(int[][] mat, int r, int c) {
        int rows = mat.length;
        int cols = mat[0].length;
        if (rows * cols != r * c) return mat;
        int[][] answer = new int[r][c];
        int i = 0;
        int j = 0;

        for (int row = 0; row < rows; row++) {
            for (int col = 0; col < cols; col++) {
                answer[i][j++] = mat[row][col];
                if (j == c) {
                    i++;
                    j = 0;
                }
            }
        }
        return answer;
    }
}