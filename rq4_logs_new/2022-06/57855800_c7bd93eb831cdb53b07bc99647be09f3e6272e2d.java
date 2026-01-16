package com.github.lucaseo90;

public class UniquePaths {
    public long uniquePaths(int m, int n) {
        int[][] path = new int[m][];

        for (int i = 0; i < m; i++) {
            path[i] = new int[n];
            for (int j = 0; j < n; j++) {
                if (i == 0 || j == 0) {
                    path[i][j] = 1;
                } else {
                    path[i][j] = path[i - 1][j] + path[i][j - 1];
                }
            }
        }
        return path[m - 1][n - 1];
    }
}