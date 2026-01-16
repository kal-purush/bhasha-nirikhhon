package dsaQuetions.medium.matrix;

import java.util.ArrayList;
import java.util.List;

public class SpiralMatrix {

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(n^2)
    //  Pattern: Layered Boundary Traversal
    public static List<Integer> getSprialListOfMatrix(int[][] matrix) {
        int totalRows = matrix.length;
        int totalColumns = matrix[0].length;
        int topRow = 0, bottomRow = totalRows - 1;
        int leftColumn = 0, rightColumn = totalColumns - 1;
        List<Integer> spiralList = new ArrayList<>();

        while ((topRow <= bottomRow) && (leftColumn <= rightColumn)) {

            //	Top row -> From left to right column
            for (int i = leftColumn; i <= rightColumn; i++) {
                spiralList.add(matrix[topRow][i]);
            }
            topRow++;

            //	Last column -> From top to bottom row
            for (int i = topRow; i <= bottomRow; i++) {
                spiralList.add(matrix[i][rightColumn]);
            }
            rightColumn--;

            //	Case: If there is a single row in matrix
            if (topRow <= bottomRow) {
                //	Bottom row -> From right to left column
                for (int i = rightColumn; i >= leftColumn; i--) {
                    spiralList.add(matrix[bottomRow][i]);
                }
                bottomRow--;
            }

            //	Case: If there is a single column in matrix
            if (leftColumn <= rightColumn) {
                //	Left column -> From bottom to top row
                for (int i = bottomRow; i >= topRow; i--) {
                    spiralList.add(matrix[i][leftColumn]);
                }
                leftColumn++;
            }
        }
        return spiralList;
    }
}