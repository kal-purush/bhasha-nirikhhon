package problem304;

public final class NumMatrix {

    private final int[][] sumMatrix;

    public NumMatrix(final int[][] matrix) {
        final int[][] sumMatrix = new int[matrix.length + 1][matrix[0].length + 1];

        for (int i = 1; i <= matrix.length; i++)
            for (int j = 1; j <= matrix[0].length; j++)
                sumMatrix[i][j] = matrix[i - 1][j - 1] + sumMatrix[i - 1][j] + sumMatrix[i][j - 1] - sumMatrix[i - 1][j - 1];

        this.sumMatrix = sumMatrix;
    }

    public int sumRegion(final int row1, final int col1, final int row2, final int col2) {
        return sumMatrix[row2 + 1][col2 + 1] + sumMatrix[row1][col1] - sumMatrix[row1][col2 + 1] - sumMatrix[row2 + 1][col1];
    }

}