/**
 * 图像顺时针旋转90度
 */
class Solution {
    public void rotate(int[][] matrix) {
        int n = matrix.length;

        int[][] temp = new int[n][n];
        for (int i = 0; i < n; ++i)
            for (int j = 0; j < n; ++j)
                temp[i][j] = matrix[i][j];

        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                matrix[j][n - 1 - i] = temp[i][j];
            }
        }

        printMatrix(matrix);
    }

    public void printMatrix(int[][] matrix) {
        int n = matrix.length;
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                System.out.print(matrix[i][j] + " ");
            }
            System.out.println();
        }
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();

        int[][] matrix = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
        s.rotate(matrix);
    }

}