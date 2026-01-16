package com.coding.challenge.matrix;

import java.util.Arrays;

class DifferenceBetweenOnesAndZerosInRowAndColumn {
	DifferenceBetweenOnesAndZerosInRowAndColumn() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] grid1 = { { 0, 1, 1 }, { 1, 0, 1 }, { 0, 0, 1 } };
		final int[][] diff1 = { { 0, 0, 4 }, { 0, 0, 4 }, { -2, -2, 2 } };
		assert Arrays.deepEquals(diff1, onesMinusZeros(grid1));

		final int[][] grid2 = { { 1, 1, 1 }, { 1, 1, 1 } };
		final int[][] diff2 = { { 5, 5, 5 }, { 5, 5, 5 } };
		assert Arrays.deepEquals(diff2, onesMinusZeros(grid2));
	}

	static int[][] onesMinusZeros(int[][] grid) {
		final int m = grid.length;
		final int n = grid[0].length;
		final int[] onesRow = new int[m];
		for (int i = 0; i < m; i++) {
			int s = 0;
			for (int j = 0; j < n; j++)
				s = s + grid[i][j];
			onesRow[i] = s;
		}

		final int[] onesCol = new int[n];
		for (int i = 0; i < n; i++) {
			int s = 0;
			for (int j = 0; j < m; j++)
				s = s + grid[j][i];
			onesCol[i] = s;
		}

		final int[][] diff = new int[m][n];
		for (int i = 0; i < m; i++)
			for (int j = 0; j < n; j++)
				diff[i][j] = 2 * onesRow[i] + 2 * onesCol[j] - m - n;

		return diff;
	}
}