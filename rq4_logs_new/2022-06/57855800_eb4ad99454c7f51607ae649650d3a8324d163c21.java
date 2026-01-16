package com.github.lucaseo90.ps.day13;

public class NumMatrix {

    private int[][] matrix;

    public NumMatrix(int[][] matrix) {
        int m = matrix.length;
        int n = matrix[0].length;

        this.matrix = new int[m][];
        for (int i = 0; i < m; i++) {
            this.matrix[i] = new int[n];
            for (int j = 0; j < n; j++) {
                this.matrix[i][j] = matrix[i][j];
            }
        }
    }

    public int sumRegion(int row1, int col1, int row2, int col2) {
        int sum = 0;
        for (int i = row1; i <= row1 + (row2 - row1); i++) {
            for (int j = col1; j <= col1 + (col2 - col1); j++) {
                sum += this.matrix[i][j];
            }
        }
        return sum;
    }
}