package com.coding.challenge.backtracking;

class UniquePathsIII {
	private static int[][] MOVES = { { 0, 1 }, { 0, -1 }, { -1, 0 }, { 1, 0 } };

	UniquePathsIII() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] grid1 = { { 1, 0, 0, 0 }, { 0, 0, 0, 0 }, { 0, 0, 2, -1 } };
		assert uniquePathsIII(grid1) == 2;

		final int[][] grid2 = { { 1, 0, 0, 0 }, { 0, 0, 0, 0 }, { 0, 0, 0, 2 } };
		assert uniquePathsIII(grid2) == 4;

		final int[][] grid3 = { { 0, 1 }, { 2, 0 } };
		assert uniquePathsIII(grid3) == 0;
	}

	static int uniquePathsIII(int[][] grid) {
		// count all the non-obstacle squares.
		final int m = grid.length;
		final int n = grid[0].length;
		int nonObstacleSquares = 0;
		int r = -1;
		int c = -1;
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				if (grid[i][j] != -1)
					nonObstacleSquares = nonObstacleSquares + 1;
				if (grid[i][j] == 1) {
					r = i;
					c = j;
				}
			}
		}

		final boolean[][] d = new boolean[m][n];
		final int[] a = new int[1];
		backtrack(grid, 1, r, c, d, a, nonObstacleSquares);
		return a[0];
	}

	private static void backtrack(int[][] grid, int l, int i, int j, boolean[][] d, int[] a, int nonObstacleSquares) {
		if (grid[i][j] == 2) {
			if (l == nonObstacleSquares)
				a[0] = a[0] + 1;
			return;
		}
		// choose.
		d[i][j] = true;
		// explore.
		for (int[] m : MOVES) {
			final int r = i + m[0];
			final int c = j + m[1];
			if (r >= 0 && r < grid.length && c >= 0 && c < grid[0].length && !d[r][c] && grid[i][j] != -1)
				backtrack(grid, l + 1, r, c, d, a, nonObstacleSquares);
		}
		// unchoose.
		d[i][j] = false;
	}
}