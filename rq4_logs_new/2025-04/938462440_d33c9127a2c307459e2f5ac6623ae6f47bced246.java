package dsaQuetions.medium.matrix;

public class RotateImage {

    //  Time complexity: O(n^2)
    //  Space complexity:
    //          Extra: O(1)
    //          Algo: O(n^2)
    //  Pattern: Transpose the matrix, reverse rows
    public static void rotateOptimised(int[][] matrix) {
        int len = matrix.length;
        //  Transposing matrix such that '1st row = 1st column'
        for (int i = 0; i < len - 1; i++) {
            for (int j = i + 1; j < len; j++) {
                int temp = matrix[i][j];
                matrix[i][j] = matrix[j][i];
                matrix[j][i] = temp;
            }
        }
        //  Reverse each row now
        for (int i = 0; i < len; i++) {
            //  Here, 'len / 2' is for right side pointer to swap with first half elements
            for (int j = 0; j < len / 2; j++) {
                int temp = matrix[i][j];
                matrix[i][j] = matrix[i][len - j - 1];
                matrix[i][len - j - 1] = temp;
            }
        }
    }
}