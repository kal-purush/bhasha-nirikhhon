package dsaQuetions.medium.matrix;

public class SetMatrixZeroes {

    public static void printMatrix(int[][] matrix) {
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[0].length; j++) {
                System.out.print(matrix[i][j] + " ");
            }
            System.out.println(); // Move to the next row
        }
    }

    //  Time complexity: O(n^2 + n^2) -> O(n^2)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(n^2)
    //  Pattern: Matrix Manipulation & Marking Technique
    public static void setMatrixZeroesBetterApproach(int[][] matrix) {
        int[] markRow = new int[matrix.length];
        int[] markCol = new int[matrix[0].length];

        //  Vertical = for rows
        for (int i = 0; i < matrix.length; i++) {
            //  Horizontal = for columns
            for (int j = 0; j < matrix[0].length; j++) {
                //  Marking current element's row & col, if its '0'.
                if (matrix[i][j] == 0) {
                    markRow[i] = 1;
                    markCol[j] = 1;
                }
            }
        }

        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[0].length; j++) {
                //  Set elements to zero in row & col of element which had value of 0
                if (markRow[i] == 1 || markCol[j] == 1) {
                    matrix[i][j] = 0;
                }
            }
        }
    }

    //  Bouncer // Yet to completed
    public static void setMatrixZeroesOptimised(int[][] matrix) {

    }
}